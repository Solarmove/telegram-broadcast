"""Build and persist progress reports independently of recipient delivery."""

from collections.abc import Callable
from datetime import timedelta
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .errors import (
    DeliveryUncertain,
    PermanentSendError,
    RateLimited,
    RecipientUnavailable,
)
from .repository import Repository, StoredNotification
from .schema import utcnow
from .transport import BroadcastTransport
from .types import BroadcastResult


class Reporter:
    def __init__(
        self,
        repository: Repository,
        sessions: async_sessionmaker[AsyncSession],
        transport: BroadcastTransport,
        sanitize_error: Callable[[Exception], str],
    ) -> None:
        self.repo = repository
        self.sessions = sessions
        self.transport = transport
        self.sanitize_error = sanitize_error

    async def send(self, job_id: UUID) -> None:
        async with self.sessions() as session:
            result = await self.repo.get_result(session, job_id)
            notifications = await self.repo.get_notifications(session, job_id)
            cooldown = await self.repo.get_cooldown(session)
        if cooldown is not None and cooldown > utcnow():
            return

        report_text = format_report(result)
        for notification in notifications:
            if notification.last_text == report_text and notification.error is None:
                continue
            await self._send_to_chat(job_id, notification, report_text)

    async def _send_to_chat(
        self, job_id: UUID, notification: StoredNotification, report_text: str
    ) -> None:
        try:
            message_id = await self.transport.report(
                notification.chat_id, report_text, notification.message_id
            )
        except RateLimited as exc:
            async with self.sessions.begin() as session:
                retry_at = utcnow() + timedelta(seconds=exc.retry_after)
                await self.repo.extend_cooldown(session, retry_at)
                await self.repo.save_report(
                    session,
                    job_id,
                    notification.chat_id,
                    message_id=notification.message_id,
                    text=notification.last_text,
                    error=self.sanitize_error(exc),
                )
        except (RecipientUnavailable, DeliveryUncertain, PermanentSendError) as exc:
            async with self.sessions.begin() as session:
                await self.repo.save_report(
                    session,
                    job_id,
                    notification.chat_id,
                    message_id=notification.message_id,
                    text=notification.last_text,
                    error=self.sanitize_error(exc),
                )
        else:
            async with self.sessions.begin() as session:
                await self.repo.save_report(
                    session,
                    job_id,
                    notification.chat_id,
                    message_id=message_id,
                    text=report_text,
                    error=None,
                )


def format_report(result: BroadcastResult) -> str:
    return (
        f"{result.title}\nID: {result.id}\nStatus: {result.status.value}\n"
        f"Total: {result.total}\nSent: {result.success}\nFailed: {result.failed}\n"
        f"Unknown: {result.unknown}\nRemaining: {result.remaining}"
    )
