"""Explicit, sequential runner with a durable journal around network calls."""

import asyncio
from collections.abc import Callable
from datetime import timedelta
from time import monotonic
from uuid import UUID

from sqlalchemy import RowMapping, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .errors import (
    DeliveryUncertain,
    PermanentSendError,
    RateLimited,
    RecipientUnavailable,
)
from .reporting import Reporter
from .repository import Repository, aware
from .schema import utcnow
from .transport import BroadcastTransport
from .types import (
    BroadcastMessage,
    BroadcastStatus,
    DeliveryStatus,
    MediaType,
    RunPolicy,
)


class Runner:
    def __init__(
        self,
        repository: Repository,
        sessions: async_sessionmaker[AsyncSession],
        transport: BroadcastTransport,
        policy: RunPolicy,
        safe_error: Callable[[Exception], str],
    ) -> None:
        self.repo, self.sessions, self.transport = repository, sessions, transport
        self.policy, self.safe_error = policy, safe_error
        self.reporter = Reporter(repository, sessions, transport, safe_error)
        self.last_report = float("-inf")

    async def run(
        self, job_id: UUID, *, wait_for_cooldown: bool, max_duration: float | None
    ) -> None:
        deadline = monotonic() + max_duration if max_duration else float("inf")
        async with self.sessions.begin() as session:
            await self.repo.job(session, job_id)
            await self.repo.recover(session, job_id)
        try:
            await self._loop(job_id, wait_for_cooldown, deadline)
        finally:
            # Returning normally preserves queued state for the host scheduler.
            # CancelledError must not trigger additional Telegram calls.
            async with self.sessions.begin() as session:
                row = await self.repo.job(session, job_id)
                if row["status"] == "running":
                    await self.repo.set_job(session, job_id, status="queued")

        await self.reporter.send(job_id)

    async def _loop(self, job_id: UUID, wait: bool, deadline: float) -> None:
        while monotonic() < deadline:
            async with self.sessions() as session:
                row = await self.repo.job(session, job_id)
                cooldown = await self.repo.cooldown(session)
            status = BroadcastStatus(row["status"])
            if status == BroadcastStatus.PAUSED or status.terminal:
                return
            retry_at = max(
                filter(None, (cooldown, aware(row["next_retry_at"]))), default=None
            )
            if retry_at and retry_at > utcnow():
                if not wait:
                    return
                await asyncio.sleep(
                    min(
                        self.policy.control_poll_interval,
                        max(0, (retry_at - utcnow()).total_seconds()),
                        max(0, deadline - monotonic()),
                    )
                )
                continue
            delivery = await self._prepare_delivery(job_id, deadline)
            if delivery is None:
                continue
            if not await self._send(job_id, delivery["chat_id"], row):
                return
            await self._pace(job_id, deadline)

    async def _pace(self, job_id: UUID, deadline: float) -> None:
        until = min(monotonic() + self.policy.send_interval, deadline)
        while monotonic() < until:
            await asyncio.sleep(
                min(self.policy.control_poll_interval, max(0, until - monotonic()))
            )
            async with self.sessions() as session:
                row = await self.repo.job(session, job_id)
            if row["status"] != "running":
                return

    async def _prepare_delivery(
        self, job_id: UUID, deadline: float
    ) -> RowMapping | None:
        async with self.sessions.begin() as session:
            table = self.repo.tables.broadcasts
            changed = await session.execute(
                update(table)
                .where(table.c.id == job_id, table.c.status.in_(["queued", "running"]))
                .values(status="running", next_retry_at=None)
                .returning(table.c.id)
            )
            if changed.scalar_one_or_none() is None:
                return None
            delivery = await self.repo.next_delivery(session, job_id)
            if delivery is None:
                await self.repo.set_job(session, job_id, status="completed")
                return None
        if monotonic() - self.last_report >= self.policy.report_interval:
            await self.reporter.send(job_id)
            self.last_report = monotonic()
        if monotonic() >= deadline:
            return None
        async with self.sessions.begin() as session:
            current = await self.repo.job(session, job_id)
            cooldown = await self.repo.cooldown(session)
            if current["status"] != "running" or (cooldown and cooldown > utcnow()):
                return None
            await self.repo.delivery(
                session, job_id, delivery["chat_id"], DeliveryStatus.SENDING
            )
        return delivery

    async def _send(self, job_id: UUID, chat_id: int, row: RowMapping) -> bool:
        message = BroadcastMessage(
            row["text"],
            row["media_url"],
            MediaType(row["media_type"]) if row["media_type"] else None,
            row["parse_mode"],
        )
        try:
            sent = await self.transport.send(chat_id, message, row["media_file_id"])
        except RateLimited as exc:
            async with self.sessions.begin() as session:
                await self.repo.delivery(
                    session, job_id, chat_id, DeliveryStatus.PENDING
                )
                await self.repo.set_cooldown(
                    session, utcnow() + timedelta(seconds=exc.retry_after)
                )
            return True
        except RecipientUnavailable as exc:
            async with self.sessions.begin() as session:
                await self.repo.delivery(
                    session,
                    job_id,
                    chat_id,
                    DeliveryStatus.FAILED,
                    error=self.safe_error(exc),
                )
            return True
        except DeliveryUncertain as exc:
            async with self.sessions.begin() as session:
                await self.repo.delivery(
                    session,
                    job_id,
                    chat_id,
                    DeliveryStatus.UNKNOWN,
                    error=self.safe_error(exc),
                )
                await self.repo.set_job(
                    session,
                    job_id,
                    next_retry_at=utcnow()
                    + timedelta(seconds=self.policy.network_retry_delay),
                )
            return False
        except PermanentSendError as exc:
            async with self.sessions.begin() as session:
                await self.repo.delivery(
                    session,
                    job_id,
                    chat_id,
                    DeliveryStatus.FAILED,
                    error=self.safe_error(exc),
                )
                await self.repo.set_job(
                    session, job_id, status="failed", error=self.safe_error(exc)
                )
            return False
        async with self.sessions.begin() as session:
            await self.repo.delivery(
                session,
                job_id,
                chat_id,
                DeliveryStatus.SUCCESS,
                message_id=sent.message_id,
            )
            if sent.media_file_id:
                await self.repo.set_job(
                    session, job_id, media_file_id=sent.media_file_id
                )
        return True
