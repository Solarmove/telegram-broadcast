"""Persisted progress notifications, isolated from recipient delivery."""

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
from .repository import Repository
from .schema import utcnow
from .transport import BroadcastTransport


class Reporter:
    def __init__(
        self,
        repository: Repository,
        sessions: async_sessionmaker[AsyncSession],
        transport: BroadcastTransport,
        safe_error: Callable[[Exception], str],
    ) -> None:
        self.repo, self.sessions, self.transport = repository, sessions, transport
        self.safe_error = safe_error

    async def send(self, job_id: UUID) -> None:
        async with self.sessions() as session:
            result = await self.repo.result(session, job_id)
            notifications = await self.repo.notifications(session, job_id)
            cooldown = await self.repo.cooldown(session)
        if cooldown and cooldown > utcnow():
            return
        text = (
            f"{result.title}\nID: {result.id}\nStatus: {result.status.value}\n"
            f"Total: {result.total}\nSent: {result.success}\nFailed: {result.failed}\n"
            f"Unknown: {result.unknown}\nRemaining: {result.remaining}"
        )
        for row in notifications:
            if row["last_text"] == text and row["error"] is None:
                continue
            try:
                message_id = await self.transport.report(
                    row["chat_id"], text, row["message_id"]
                )
            except RateLimited as exc:
                async with self.sessions.begin() as session:
                    await self.repo.set_cooldown(
                        session, utcnow() + timedelta(seconds=exc.retry_after)
                    )
                    await self.repo.save_report(
                        session, job_id, row["chat_id"], error=self.safe_error(exc)
                    )
                return
            except (RecipientUnavailable, DeliveryUncertain, PermanentSendError) as exc:
                async with self.sessions.begin() as session:
                    await self.repo.save_report(
                        session, job_id, row["chat_id"], error=self.safe_error(exc)
                    )
            else:
                async with self.sessions.begin() as session:
                    await self.repo.save_report(
                        session,
                        job_id,
                        row["chat_id"],
                        message_id=message_id,
                        last_text=text,
                        error=None,
                    )
