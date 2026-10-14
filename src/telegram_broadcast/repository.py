"""Persistence boundary: own every SQL statement and state transition here."""

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

from aiogram.enums import ContentType
from sqlalchemy import func, insert, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from .errors import BroadcastNotFound, InvalidTransition, WrongBot
from .schema import BroadcastTables, utcnow
from .types import (
    BroadcastMedia,
    BroadcastMessage,
    BroadcastResult,
    BroadcastStatus,
    DeliveryResult,
    DeliveryStatus,
    NotificationRecipient,
    NotificationResult,
)


def _aware(value: datetime | None) -> datetime | None:
    if value is not None and value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def _required_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


@dataclass(frozen=True)
class StoredBroadcast:
    id: UUID
    bot_id: int
    title: str
    message: BroadcastMessage
    media_file_id: str | None
    status: BroadcastStatus
    total: int
    success: int
    failed: int
    unknown: int
    error: str | None
    next_retry_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class PendingDelivery:
    chat_id: int


@dataclass(frozen=True)
class StoredNotification:
    chat_id: int
    thread_id: int | None
    message_id: int | None
    error: str | None
    last_text: str | None
    updated_at: datetime | None


class Repository:
    def __init__(self, tables: BroadcastTables, bot_id: int) -> None:
        self.tables = tables
        self.bot_id = bot_id

    async def create_broadcast(
        self,
        session: AsyncSession,
        message: BroadcastMessage,
        recipients: list[int],
        notification_recipients: list[NotificationRecipient],
        title: str,
    ) -> UUID:
        job_id = uuid4()
        jobs = self.tables.broadcasts
        await session.execute(
            insert(jobs).values(
                id=job_id,
                bot_id=self.bot_id,
                title=title,
                text=message.text,
                media_content_type=(
                    message.media.content_type.value if message.media else None
                ),
                media_source_file_id=(message.media.file_id if message.media else None),
                media_url=message.media.url if message.media else None,
                media_path=message.media.path_to_file if message.media else None,
                parse_mode=message.parse_mode,
                status=BroadcastStatus.QUEUED.value,
                total=len(recipients),
            )
        )
        deliveries = self.tables.deliveries
        for start in range(0, len(recipients), 1000):
            await session.execute(
                insert(deliveries),
                [
                    {
                        "broadcast_id": job_id,
                        "chat_id": chat_id,
                        "status": DeliveryStatus.PENDING.value,
                    }
                    for chat_id in recipients[start : start + 1000]
                ],
            )
        notifications = self.tables.notifications
        for start in range(0, len(notification_recipients), 1000):
            await session.execute(
                insert(notifications),
                [
                    {
                        "broadcast_id": job_id,
                        "chat_id": recipient.chat_id,
                        "thread_id": recipient.thread_id or 0,
                    }
                    for recipient in notification_recipients[start : start + 1000]
                ],
            )
        return job_id

    async def get_broadcast(
        self, session: AsyncSession, job_id: UUID
    ) -> StoredBroadcast:
        jobs = self.tables.broadcasts
        row = (
            (await session.execute(select(jobs).where(jobs.c.id == job_id)))
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise BroadcastNotFound(str(job_id))
        if row["bot_id"] != self.bot_id:
            raise WrongBot("Broadcast belongs to another bot")
        return StoredBroadcast(
            id=row["id"],
            bot_id=row["bot_id"],
            title=row["title"],
            message=BroadcastMessage(
                text=row["text"],
                media=(
                    BroadcastMedia(
                        content_type=ContentType(row["media_content_type"]),
                        file_id=row["media_source_file_id"],
                        url=row["media_url"],
                        path_to_file=row["media_path"],
                    )
                    if row["media_content_type"]
                    else None
                ),
                parse_mode=row["parse_mode"],
            ),
            media_file_id=row["media_file_id"],
            status=BroadcastStatus(row["status"]),
            total=row["total"],
            success=row["success"],
            failed=row["failed"],
            unknown=row["unknown"],
            error=row["error"],
            next_retry_at=_aware(row["next_retry_at"]),
            created_at=_required_datetime(row["created_at"]),
            updated_at=_required_datetime(row["updated_at"]),
        )

    async def get_result(self, session: AsyncSession, job_id: UUID) -> BroadcastResult:
        broadcast = await self.get_broadcast(session, job_id)
        notifications = await self.get_notifications(session, job_id)
        retry_at = max(
            filter(None, (broadcast.next_retry_at, await self.get_cooldown(session))),
            default=None,
        )
        if retry_at is not None and retry_at <= utcnow():
            retry_at = None
        return BroadcastResult(
            id=broadcast.id,
            bot_id=broadcast.bot_id,
            title=broadcast.title,
            status=broadcast.status,
            total=broadcast.total,
            success=broadcast.success,
            failed=broadcast.failed,
            unknown=broadcast.unknown,
            error=broadcast.error,
            next_retry_at=retry_at,
            created_at=broadcast.created_at,
            updated_at=broadcast.updated_at,
            notifications=tuple(
                NotificationResult(
                    item.chat_id,
                    item.thread_id,
                    item.message_id,
                    item.error,
                    item.updated_at,
                )
                for item in notifications
            ),
        )

    async def get_notifications(
        self, session: AsyncSession, job_id: UUID
    ) -> tuple[StoredNotification, ...]:
        table = self.tables.notifications
        rows = (
            await session.execute(
                select(table)
                .where(table.c.broadcast_id == job_id)
                .order_by(table.c.chat_id)
            )
        ).mappings()
        return tuple(
            StoredNotification(
                chat_id=row["chat_id"],
                thread_id=row["thread_id"] or None,
                message_id=row["message_id"],
                error=row["error"],
                last_text=row["last_text"],
                updated_at=_aware(row["updated_at"]),
            )
            for row in rows
        )

    async def transition_broadcast(
        self,
        session: AsyncSession,
        job_id: UUID,
        allowed: tuple[BroadcastStatus, ...],
        target: BroadcastStatus,
    ) -> None:
        await self.get_broadcast(session, job_id)
        table = self.tables.broadcasts
        updated = await session.execute(
            update(table)
            .where(
                table.c.id == job_id,
                table.c.status.in_([status.value for status in allowed]),
            )
            .values(status=target.value, updated_at=utcnow())
            .returning(table.c.id)
        )
        if updated.scalar_one_or_none() is None:
            raise InvalidTransition(f"Cannot change broadcast to {target.value}")

    async def release_runner(self, session: AsyncSession, job_id: UUID) -> None:
        jobs = self.tables.broadcasts
        await session.execute(
            update(jobs)
            .where(
                jobs.c.id == job_id,
                jobs.c.bot_id == self.bot_id,
                jobs.c.status == BroadcastStatus.RUNNING.value,
            )
            .values(status=BroadcastStatus.QUEUED.value, updated_at=utcnow())
        )

    async def claim_next_delivery(
        self, session: AsyncSession, job_id: UUID
    ) -> PendingDelivery | None:
        jobs = self.tables.broadcasts
        claimed = await session.execute(
            update(jobs)
            .where(
                jobs.c.id == job_id,
                jobs.c.bot_id == self.bot_id,
                jobs.c.status.in_(
                    [BroadcastStatus.QUEUED.value, BroadcastStatus.RUNNING.value]
                ),
            )
            .values(
                status=BroadcastStatus.RUNNING.value,
                next_retry_at=None,
                updated_at=utcnow(),
            )
            .returning(jobs.c.id)
        )
        if claimed.scalar_one_or_none() is None:
            return None
        deliveries = self.tables.deliveries
        row = (
            await session.execute(
                select(deliveries.c.chat_id)
                .where(
                    deliveries.c.broadcast_id == job_id,
                    deliveries.c.status == DeliveryStatus.PENDING.value,
                )
                .order_by(deliveries.c.chat_id)
                .limit(1)
            )
        ).one_or_none()
        if row is None:
            await session.execute(
                update(jobs)
                .where(jobs.c.id == job_id)
                .values(status=BroadcastStatus.COMPLETED.value, updated_at=utcnow())
            )
            return None
        return PendingDelivery(chat_id=row.chat_id)

    async def mark_sending(
        self, session: AsyncSession, job_id: UUID, chat_id: int
    ) -> bool:
        delivery = self.tables.deliveries
        jobs = self.tables.broadcasts
        running_job = select(jobs.c.id).where(
            jobs.c.id == job_id,
            jobs.c.bot_id == self.bot_id,
            jobs.c.status == BroadcastStatus.RUNNING.value,
        )
        updated = await session.execute(
            update(delivery)
            .where(
                delivery.c.broadcast_id == job_id,
                delivery.c.chat_id == chat_id,
                delivery.c.status == DeliveryStatus.PENDING.value,
                running_job.exists(),
            )
            .values(
                status=DeliveryStatus.SENDING.value, error=None, updated_at=utcnow()
            )
            .returning(delivery.c.chat_id)
        )
        return updated.scalar_one_or_none() is not None

    async def finish_delivery(
        self,
        session: AsyncSession,
        job_id: UUID,
        chat_id: int,
        status: DeliveryStatus,
        *,
        message_id: int | None = None,
        error: str | None = None,
    ) -> bool:
        if status not in (
            DeliveryStatus.SUCCESS,
            DeliveryStatus.FAILED,
            DeliveryStatus.UNKNOWN,
        ):
            raise ValueError("finish_delivery requires a terminal delivery status")
        table = self.tables.deliveries
        updated = await session.execute(
            update(table)
            .where(
                table.c.broadcast_id == job_id,
                table.c.chat_id == chat_id,
                table.c.status == DeliveryStatus.SENDING.value,
            )
            .values(
                status=status.value,
                message_id=message_id,
                error=error,
                updated_at=utcnow(),
            )
            .returning(table.c.chat_id)
        )
        if updated.scalar_one_or_none() is None:
            return False
        jobs = self.tables.broadcasts
        await session.execute(
            update(jobs)
            .where(jobs.c.id == job_id, jobs.c.bot_id == self.bot_id)
            .values({status.value: jobs.c[status.value] + 1, "updated_at": utcnow()})
        )
        return True

    async def retry_delivery(
        self, session: AsyncSession, job_id: UUID, chat_id: int
    ) -> bool:
        table = self.tables.deliveries
        updated = await session.execute(
            update(table)
            .where(
                table.c.broadcast_id == job_id,
                table.c.chat_id == chat_id,
                table.c.status == DeliveryStatus.SENDING.value,
            )
            .values(
                status=DeliveryStatus.PENDING.value, error=None, updated_at=utcnow()
            )
            .returning(table.c.chat_id)
        )
        return updated.scalar_one_or_none() is not None

    async def recover_interrupted_deliveries(
        self, session: AsyncSession, job_id: UUID
    ) -> None:
        deliveries = self.tables.deliveries
        jobs = self.tables.broadcasts
        count = await session.scalar(
            select(func.count())
            .select_from(deliveries)
            .where(
                deliveries.c.broadcast_id == job_id,
                deliveries.c.status == DeliveryStatus.SENDING.value,
            )
        )
        if count:
            await session.execute(
                update(deliveries)
                .where(
                    deliveries.c.broadcast_id == job_id,
                    deliveries.c.status == DeliveryStatus.SENDING.value,
                )
                .values(
                    status=DeliveryStatus.UNKNOWN.value,
                    error="Interrupted during delivery; Telegram outcome is unknown",
                    updated_at=utcnow(),
                )
            )
            await session.execute(
                update(jobs)
                .where(jobs.c.id == job_id, jobs.c.bot_id == self.bot_id)
                .values(unknown=jobs.c.unknown + count, updated_at=utcnow())
            )

    async def get_cooldown(self, session: AsyncSession) -> datetime | None:
        value = await session.scalar(
            select(self.tables.cooldowns.c.retry_at).where(
                self.tables.cooldowns.c.bot_id == self.bot_id
            )
        )
        return _aware(value)

    async def extend_cooldown(self, session: AsyncSession, retry_at: datetime) -> None:
        table = self.tables.cooldowns
        await session.execute(
            sqlite_insert(table)
            .values(bot_id=self.bot_id, retry_at=retry_at)
            .on_conflict_do_update(
                index_elements=[table.c.bot_id],
                set_={
                    "retry_at": func.max(table.c.retry_at, retry_at),
                },
            )
        )

    async def record_media_file_id(
        self, session: AsyncSession, job_id: UUID, file_id: str
    ) -> None:
        jobs = self.tables.broadcasts
        await session.execute(
            update(jobs)
            .where(jobs.c.id == job_id, jobs.c.bot_id == self.bot_id)
            .values(media_file_id=file_id, updated_at=utcnow())
        )

    async def record_retry_after(
        self, session: AsyncSession, job_id: UUID, retry_at: datetime
    ) -> None:
        jobs = self.tables.broadcasts
        await session.execute(
            update(jobs)
            .where(jobs.c.id == job_id, jobs.c.bot_id == self.bot_id)
            .values(next_retry_at=retry_at, updated_at=utcnow())
        )

    async def reset_retry_after(self, session: AsyncSession, job_id: UUID) -> None:
        jobs = self.tables.broadcasts
        await session.execute(
            update(jobs)
            .where(jobs.c.id == job_id, jobs.c.bot_id == self.bot_id)
            .values(next_retry_at=None)
        )

    async def set_failed(self, session: AsyncSession, job_id: UUID, error: str) -> None:
        jobs = self.tables.broadcasts
        await session.execute(
            update(jobs)
            .where(jobs.c.id == job_id, jobs.c.bot_id == self.bot_id)
            .values(
                status=BroadcastStatus.FAILED.value, error=error, updated_at=utcnow()
            )
        )

    async def list_deliveries(
        self, session: AsyncSession, job_id: UUID, limit: int, offset: int
    ) -> tuple[DeliveryResult, ...]:
        await self.get_broadcast(session, job_id)
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
                chat_id=row["chat_id"],
                status=DeliveryStatus(row["status"]),
                message_id=row["message_id"],
                error=row["error"],
                updated_at=_required_datetime(row["updated_at"]),
            )
            for row in rows
        )

    async def save_report(
        self,
        session: AsyncSession,
        job_id: UUID,
        chat_id: int,
        thread_id: int | None,
        *,
        message_id: int | None,
        text: str | None,
        error: str | None,
    ) -> None:
        table = self.tables.notifications
        await session.execute(
            update(table)
            .where(
                table.c.broadcast_id == job_id,
                table.c.chat_id == chat_id,
                table.c.thread_id == (thread_id or 0),
            )
            .values(
                message_id=message_id, last_text=text, error=error, updated_at=utcnow()
            )
        )
