"""Public facade and resource lifecycle."""

import re
from collections.abc import Iterable
from math import isfinite
from pathlib import Path
from uuid import UUID, uuid4

from aiogram.utils.token import extract_bot_id, validate_token
from sqlalchemy import insert, update

from .database import Database
from .errors import InvalidTransition
from .locking import FileBotLock
from .repository import Repository
from .runner import Runner
from .transport import AiogramTransport, BroadcastTransport
from .types import (
    BroadcastMessage,
    BroadcastResult,
    BroadcastStatus,
    DeliveryResult,
    RunPolicy,
)


def _ids(values: Iterable[int]) -> list[int]:
    result = list(values)
    if any(
        type(value) is not int or value == 0 or not -(2**63) <= value < 2**63
        for value in result
    ):
        raise ValueError("Chat IDs must be nonzero signed 64-bit integers")
    return list(dict.fromkeys(result))


class BroadcastClient:
    def __init__(
        self,
        bot_token: str,
        *,
        database_path: str | Path = "broadcasts.sqlite3",
        policy: RunPolicy | None = None,
        transport: BroadcastTransport | None = None,
    ) -> None:
        validate_token(bot_token)
        self.bot_id = extract_bot_id(bot_token)
        self._database = Database(database_path)
        self.database_path = self._database.path
        self.tables = self._database.tables
        self._sessions = self._database.sessions
        self._repository = Repository(self.tables, self.bot_id)
        self._lock = FileBotLock(self.database_path)
        self._owned_transport = (
            AiogramTransport(bot_token) if transport is None else None
        )
        if transport is not None:
            self._transport = transport
        else:
            assert self._owned_transport is not None
            self._transport = self._owned_transport
        self._policy = policy or RunPolicy()
        self._token = bot_token

    async def __aenter__(self) -> "BroadcastClient":
        try:
            await self._database.initialize()
        except BaseException:
            await self.close()
            raise
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    async def close(self) -> None:
        try:
            if self._owned_transport is not None:
                await self._owned_transport.close()
        finally:
            await self._database.close()

    async def create(
        self,
        message: BroadcastMessage,
        recipient_ids: Iterable[int],
        *,
        notification_chat_ids: Iterable[int] = (),
        title: str = "Broadcast",
    ) -> UUID:
        recipients, notifications = _ids(recipient_ids), _ids(notification_chat_ids)
        if not recipients:
            raise ValueError("At least one recipient is required")
        if not title.strip() or len(title) > 200:
            raise ValueError("title must contain 1 to 200 characters")
        await self._database.initialize()
        job_id = uuid4()
        async with self._sessions.begin() as session:
            await session.execute(
                insert(self.tables.broadcasts).values(
                    id=job_id,
                    bot_id=self.bot_id,
                    title=title,
                    text=message.text,
                    media_url=message.media_url,
                    media_type=message.media_type.value if message.media_type else None,
                    parse_mode=message.parse_mode,
                    status="queued",
                    total=len(recipients),
                )
            )
            # Keep insert batches bounded for large recipient lists.
            for start in range(0, len(recipients), 1000):
                await session.execute(
                    insert(self.tables.deliveries),
                    [
                        dict(broadcast_id=job_id, chat_id=value, status="pending")
                        for value in recipients[start : start + 1000]
                    ],
                )
            for start in range(0, len(notifications), 1000):
                await session.execute(
                    insert(self.tables.notifications),
                    [
                        dict(broadcast_id=job_id, chat_id=value)
                        for value in notifications[start : start + 1000]
                    ],
                )
        return job_id

    async def get_result(self, job_id: UUID) -> BroadcastResult:
        await self._database.initialize()
        async with self._sessions() as session:
            return await self._repository.result(session, job_id)

    async def get_deliveries(
        self, job_id: UUID, *, limit: int = 100, offset: int = 0
    ) -> tuple[DeliveryResult, ...]:
        if not 1 <= limit <= 1000 or offset < 0:
            raise ValueError("limit must be 1..1000 and offset nonnegative")
        await self._database.initialize()
        async with self._sessions() as session:
            return await self._repository.deliveries(session, job_id, limit, offset)

    async def pause(self, job_id: UUID) -> None:
        await self._transition(
            job_id,
            (BroadcastStatus.QUEUED, BroadcastStatus.RUNNING),
            BroadcastStatus.PAUSED,
        )

    async def resume(self, job_id: UUID) -> None:
        await self._transition(
            job_id, (BroadcastStatus.PAUSED,), BroadcastStatus.QUEUED
        )

    async def cancel(self, job_id: UUID) -> None:
        await self._transition(
            job_id,
            (BroadcastStatus.QUEUED, BroadcastStatus.RUNNING, BroadcastStatus.PAUSED),
            BroadcastStatus.CANCELLED,
        )

    async def _transition(
        self,
        job_id: UUID,
        allowed: tuple[BroadcastStatus, ...],
        target: BroadcastStatus,
    ) -> None:
        await self._database.initialize()
        async with self._sessions.begin() as session:
            await self._repository.job(session, job_id)
            table = self.tables.broadcasts
            result = await session.execute(
                update(table)
                .where(
                    table.c.id == job_id, table.c.status.in_([s.value for s in allowed])
                )
                .values(status=target.value)
                .returning(table.c.id)
            )
            if result.scalar_one_or_none() is None:
                raise InvalidTransition(f"Cannot change broadcast to {target.value}")

    def _safe_error(self, exc: Exception) -> str:
        return re.sub(
            r"\d{5,}:[A-Za-z0-9_-]{20,}",
            "[redacted]",
            str(exc).replace(self._token, "[redacted]"),
        )[:2000]

    async def run(
        self,
        job_id: UUID,
        *,
        wait_for_cooldown: bool = True,
        max_duration: float | None = None,
    ) -> BroadcastResult:
        if max_duration is not None and (
            not isfinite(max_duration) or max_duration <= 0
        ):
            raise ValueError("max_duration must be finite and positive")
        await self._database.initialize()
        runner = Runner(
            self._repository,
            self._sessions,
            self._transport,
            self._policy,
            self._safe_error,
        )
        async with self._lock.acquire(self.bot_id):
            await runner.run(
                job_id, wait_for_cooldown=wait_for_cooldown, max_duration=max_duration
            )
        return await self.get_result(job_id)
