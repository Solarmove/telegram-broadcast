"""SQL persistence. Transactions are owned by the caller."""

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any, overload
from uuid import UUID

from sqlalchemy import RowMapping, func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from .errors import BroadcastNotFound, WrongBot
from .schema import BroadcastTables, utcnow
from .types import (
    BroadcastResult,
    BroadcastStatus,
    DeliveryResult,
    DeliveryStatus,
    NotificationResult,
)


@overload
def aware(value: datetime) -> datetime: ...


@overload
def aware(value: None) -> None: ...


def aware(value: datetime | None) -> datetime | None:
    return (
        value.replace(tzinfo=UTC)
        if value is not None and value.tzinfo is None
        else value
    )


class Repository:
    def __init__(self, tables: BroadcastTables, bot_id: int) -> None:
        self.tables = tables
        self.bot_id = bot_id

    async def job(self, session: AsyncSession, job_id: UUID) -> RowMapping:
        row = (
            (
                await session.execute(
                    select(self.tables.broadcasts).where(
                        self.tables.broadcasts.c.id == job_id
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise BroadcastNotFound(str(job_id))
        if row["bot_id"] != self.bot_id:
            raise WrongBot("Broadcast belongs to another bot")
        return row

    async def result(self, session: AsyncSession, job_id: UUID) -> BroadcastResult:
        row = await self.job(session, job_id)
        notifications = await self.notifications(session, job_id)
        retry_at = max(
            filter(None, (aware(row["next_retry_at"]), await self.cooldown(session))),
            default=None,
        )
        if retry_at is not None and retry_at <= utcnow():
            retry_at = None
        return BroadcastResult(
            id=row["id"],
            bot_id=row["bot_id"],
            title=row["title"],
            status=BroadcastStatus(row["status"]),
            total=row["total"],
            success=row["success"],
            failed=row["failed"],
            unknown=row["unknown"],
            error=row["error"],
            next_retry_at=retry_at,
            created_at=aware(row["created_at"]),
            updated_at=aware(row["updated_at"]),
            notifications=tuple(
                NotificationResult(
                    r["chat_id"], r["message_id"], r["error"], aware(r["updated_at"])
                )
                for r in notifications
            ),
        )

    async def notifications(
        self, session: AsyncSession, job_id: UUID
    ) -> Sequence[RowMapping]:
        table = self.tables.notifications
        return (
            (
                await session.execute(
                    select(table)
                    .where(table.c.broadcast_id == job_id)
                    .order_by(table.c.chat_id)
                )
            )
            .mappings()
            .all()
        )

    async def set_job(self, session: AsyncSession, job_id: UUID, **values: Any) -> None:
        await session.execute(
            update(self.tables.broadcasts)
            .where(self.tables.broadcasts.c.id == job_id)
            .values(**values)
        )

    async def next_delivery(
        self, session: AsyncSession, job_id: UUID
    ) -> RowMapping | None:
        table = self.tables.deliveries
        return (
            (
                await session.execute(
                    select(table)
                    .where(table.c.broadcast_id == job_id, table.c.status == "pending")
                    .order_by(table.c.chat_id)
                    .limit(1)
                )
            )
            .mappings()
            .one_or_none()
        )

    async def delivery(
        self,
        session: AsyncSession,
        job_id: UUID,
        chat_id: int,
        status: DeliveryStatus,
        *,
        message_id: int | None = None,
        error: str | None = None,
    ) -> None:
        table = self.tables.deliveries
        await session.execute(
            update(table)
            .where(table.c.broadcast_id == job_id, table.c.chat_id == chat_id)
            .values(status=status.value, message_id=message_id, error=error)
        )
        if status in (
            DeliveryStatus.SUCCESS,
            DeliveryStatus.FAILED,
            DeliveryStatus.UNKNOWN,
        ):
            jobs = self.tables.broadcasts
            column = jobs.c[status.value]
            await self.set_job(session, job_id, **{status.value: column + 1})

    async def recover(self, session: AsyncSession, job_id: UUID) -> None:
        table = self.tables.deliveries
        count = await session.scalar(
            select(func.count())
            .select_from(table)
            .where(table.c.broadcast_id == job_id, table.c.status == "sending")
        )
        if count:
            await session.execute(
                update(table)
                .where(table.c.broadcast_id == job_id, table.c.status == "sending")
                .values(
                    status="unknown",
                    error="Interrupted during delivery; Telegram outcome is unknown",
                )
            )
            jobs = self.tables.broadcasts
            await self.set_job(session, job_id, unknown=jobs.c.unknown + count)

    async def cooldown(self, session: AsyncSession) -> datetime | None:
        return aware(
            await session.scalar(
                select(self.tables.cooldowns.c.retry_at).where(
                    self.tables.cooldowns.c.bot_id == self.bot_id
                )
            )
        )

    async def set_cooldown(self, session: AsyncSession, retry_at: datetime) -> None:
        table = self.tables.cooldowns
        previous = await self.cooldown(session)
        if previous is None:
            await session.execute(
                insert(table).values(bot_id=self.bot_id, retry_at=retry_at)
            )
        elif previous < retry_at:
            await session.execute(
                update(table)
                .where(table.c.bot_id == self.bot_id)
                .values(retry_at=retry_at)
            )

    async def deliveries(
        self, session: AsyncSession, job_id: UUID, limit: int, offset: int
    ) -> tuple[DeliveryResult, ...]:
        await self.job(session, job_id)
        table = self.tables.deliveries
        rows = (
            await session.execute(
                select(table)
                .where(table.c.broadcast_id == job_id)
                .order_by(table.c.chat_id)
                .limit(limit)
                .offset(offset)
            )
        ).mappings()
        return tuple(
            DeliveryResult(
                r["chat_id"],
                DeliveryStatus(r["status"]),
                r["message_id"],
                r["error"],
                aware(r["updated_at"]),
            )
            for r in rows
        )

    async def save_report(
        self, session: AsyncSession, job_id: UUID, chat_id: int, **values
    ) -> None:
        table = self.tables.notifications
        await session.execute(
            update(table)
            .where(table.c.broadcast_id == job_id, table.c.chat_id == chat_id)
            .values(updated_at=utcnow(), **values)
        )
