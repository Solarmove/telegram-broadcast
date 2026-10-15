"""Drive broadcast transitions; delegate all persistence to Repository."""

import asyncio
import logging
from collections.abc import Callable
from datetime import timedelta
from time import monotonic
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .errors import (
    DeliveryUncertain,
    PermanentSendError,
    RateLimited,
    RecipientUnavailable,
)
from .reporting import Reporter
from .repository import PendingDelivery, Repository, StoredBroadcast
from .schema import utcnow
from .transport import BroadcastTransport
from .types import BroadcastStatus, DeliveryStatus, RunPolicy

logger = logging.getLogger(__name__)


class Runner:
    def __init__(
        self,
        repository: Repository,
        sessions: async_sessionmaker[AsyncSession],
        transport: BroadcastTransport,
        policy: RunPolicy,
        sanitize_error: Callable[[Exception], str],
    ) -> None:
        self.repo = repository
        self.sessions = sessions
        self.transport = transport
        self.policy = policy
        self.sanitize_error = sanitize_error
        self.reporter = Reporter(repository, sessions, transport, sanitize_error)
        self._report_task: asyncio.Task[None] | None = None

    async def run(
        self, job_id: UUID, *, wait_for_cooldown: bool, max_duration: float | None
    ) -> None:
        deadline = (
            monotonic() + max_duration if max_duration is not None else float("inf")
        )
        async with self.sessions.begin() as session:
            broadcast = await self.repo.get_broadcast(session, job_id)
            await self.repo.recover_interrupted_deliveries(session, job_id)
            await self.repo.start_run(session, job_id)

        if broadcast.status.terminal or broadcast.status == BroadcastStatus.PAUSED:
            await self.reporter.send(job_id)
            return

        await self.reporter.send(job_id)
        stop_reports = asyncio.Event()
        self._report_task = asyncio.create_task(
            self._report_periodically(job_id, stop_reports)
        )

        # Leave RUNNING persisted on cancellation or an unexpected error.
        # The next process can classify a stranded SENDING delivery as UNKNOWN.
        try:
            await self._loop(job_id, wait_for_cooldown, deadline)
        except BaseException as exc:
            self._report_task.cancel()
            await asyncio.gather(self._report_task, return_exceptions=True)
            if isinstance(exc, Exception):
                try:
                    await self.reporter.send(job_id, stop_error=self.sanitize_error(exc))
                except Exception:
                    logger.exception("Cannot report broadcast %s stopping error", job_id)
            raise
        else:
            stop_reports.set()
            await self._report_task
        async with self.sessions.begin() as session:
            await self.repo.release_runner(session, job_id)
        await self.reporter.send(job_id)

    async def _report_periodically(self, job_id: UUID, stop: asyncio.Event) -> None:
        interval = max(self.policy.report_interval, self.policy.control_poll_interval)
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except TimeoutError:
                await self.reporter.send(job_id)

    async def _loop(self, job_id: UUID, wait: bool, deadline: float) -> None:
        while monotonic() < deadline:
            if self._report_task is not None and self._report_task.done():
                self._report_task.result()
            async with self.sessions() as session:
                broadcast = await self.repo.get_broadcast(session, job_id)
                cooldown = await self.repo.get_cooldown(session)
            if broadcast.status == BroadcastStatus.PAUSED or broadcast.status.terminal:
                return
            retry_at = max(
                filter(None, (cooldown, broadcast.next_retry_at)), default=None
            )
            if retry_at is not None and retry_at > utcnow():
                if not wait:
                    return
                remaining = (retry_at - utcnow()).total_seconds()
                await asyncio.sleep(
                    min(
                        self.policy.control_poll_interval,
                        remaining,
                        max(0, deadline - monotonic()),
                    )
                )
                continue

            delivery = await self._prepare_delivery(job_id, deadline)
            if delivery is None:
                continue
            if not await self._send(job_id, broadcast, delivery):
                return
            await self._pace(job_id, deadline)

    async def _pace(self, job_id: UUID, deadline: float) -> None:
        until = min(monotonic() + self.policy.send_interval, deadline)
        while monotonic() < until:
            await asyncio.sleep(
                min(self.policy.control_poll_interval, max(0, until - monotonic()))
            )
            async with self.sessions() as session:
                broadcast = await self.repo.get_broadcast(session, job_id)
            if broadcast.status != BroadcastStatus.RUNNING:
                return

    async def _prepare_delivery(
        self, job_id: UUID, deadline: float
    ) -> PendingDelivery | None:
        async with self.sessions.begin() as session:
            delivery = await self.repo.claim_next_delivery(session, job_id)
        if delivery is None:
            return None

        if monotonic() >= deadline:
            return None
        async with self.sessions.begin() as session:
            cooldown = await self.repo.get_cooldown(session)
            if cooldown is not None and cooldown > utcnow():
                return None
            if not await self.repo.mark_sending(session, job_id, delivery.chat_id):
                return None
        return delivery

    async def _send(
        self, job_id: UUID, broadcast: StoredBroadcast, delivery: PendingDelivery
    ) -> bool:
        chat_id = delivery.chat_id
        try:
            sent = await self.transport.send(
                chat_id, broadcast.message, broadcast.media_file_id
            )
        except RateLimited as exc:
            async with self.sessions.begin() as session:
                await self.repo.retry_delivery(session, job_id, chat_id)
                retry_at = utcnow() + timedelta(seconds=exc.retry_after)
                await self.repo.extend_cooldown(session, retry_at)
                await self.repo.record_retry_after(session, job_id, retry_at)
            return True
        except (RecipientUnavailable, PermanentSendError) as exc:
            async with self.sessions.begin() as session:
                await self.repo.finish_delivery(
                    session,
                    job_id,
                    chat_id,
                    DeliveryStatus.FAILED,
                    error=self.sanitize_error(exc),
                )
            return True
        except DeliveryUncertain as exc:
            async with self.sessions.begin() as session:
                await self.repo.finish_delivery(
                    session,
                    job_id,
                    chat_id,
                    DeliveryStatus.UNKNOWN,
                    error=self.sanitize_error(exc),
                )
                retry_at = utcnow() + timedelta(seconds=self.policy.network_retry_delay)
                await self.repo.record_retry_after(session, job_id, retry_at)
            return True

        async with self.sessions.begin() as session:
            await self.repo.finish_delivery(
                session,
                job_id,
                chat_id,
                DeliveryStatus.SUCCESS,
                message_id=sent.message_id,
            )
            if sent.media_file_id:
                await self.repo.record_media_file_id(
                    session, job_id, sent.media_file_id
                )
        return True
