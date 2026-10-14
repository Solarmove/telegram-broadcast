"""Private schema owned by the library, independent of application metadata."""

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    Uuid,
    func,
)


def utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class BroadcastTables:
    metadata: MetaData
    broadcasts: Table
    deliveries: Table
    notifications: Table
    cooldowns: Table

    @property
    def all(self) -> tuple[Table, ...]:
        return (self.broadcasts, self.deliveries, self.notifications, self.cooldowns)


def build_tables() -> BroadcastTables:
    """Each client gets its own metadata; persisted table names remain stable."""
    names = [
        f"tg_broadcast_{suffix}"
        for suffix in ("jobs", "deliveries", "notifications", "cooldowns")
    ]
    return _build_tables(MetaData(), names)


def _build_tables(metadata: MetaData, names: list[str]) -> BroadcastTables:
    jobs = Table(
        names[0],
        metadata,
        Column("id", Uuid, primary_key=True),
        Column("bot_id", BigInteger, nullable=False),
        Column("title", String(200), nullable=False),
        Column("text", Text),
        Column("media_content_type", String(32)),
        Column("media_source_file_id", Text),
        Column("media_url", Text),
        Column("media_path", Text),
        Column("parse_mode", String(16)),
        Column("media_file_id", Text),
        Column("status", String(16), nullable=False),
        Column("total", Integer, nullable=False),
        Column("success", Integer, nullable=False, default=0),
        Column("failed", Integer, nullable=False, default=0),
        Column("unknown", Integer, nullable=False, default=0),
        Column("error", Text),
        Column("next_retry_at", DateTime(timezone=True)),
        Column("started_at", DateTime(timezone=True)),
        Column("finished_at", DateTime(timezone=True)),
        Column(
            "created_at",
            DateTime(timezone=True),
            nullable=False,
            default=utcnow,
            server_default=func.now(),
        ),
        Column(
            "updated_at",
            DateTime(timezone=True),
            nullable=False,
            default=utcnow,
            onupdate=utcnow,
            server_default=func.now(),
        ),
        CheckConstraint(
            "status IN ('queued','running','paused','completed','cancelled','failed')",
            name=f"{names[0]}_status",
        ),
    )
    deliveries = Table(
        names[1],
        metadata,
        Column(
            "broadcast_id",
            Uuid,
            ForeignKey(jobs.c.id, ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("chat_id", BigInteger, primary_key=True),
        Column("status", String(16), nullable=False),
        Column("message_id", BigInteger),
        Column("error", Text),
        Column(
            "updated_at",
            DateTime(timezone=True),
            nullable=False,
            default=utcnow,
            onupdate=utcnow,
            server_default=func.now(),
        ),
        CheckConstraint(
            "status IN ('pending','sending','success','failed','unknown')",
            name=f"{names[1]}_status",
        ),
    )
    notifications = Table(
        names[2],
        metadata,
        Column(
            "broadcast_id",
            Uuid,
            ForeignKey(jobs.c.id, ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("chat_id", BigInteger, primary_key=True),
        Column("thread_id", BigInteger, primary_key=True, default=0),
        Column("message_id", BigInteger),
        Column("error", Text),
        Column("last_text", Text),
        Column("updated_at", DateTime(timezone=True)),
    )
    cooldowns = Table(
        names[3],
        metadata,
        Column("bot_id", BigInteger, primary_key=True, autoincrement=False),
        Column("retry_at", DateTime(timezone=True), nullable=False),
    )
    Index(
        f"{names[1]}_pending",
        deliveries.c.broadcast_id,
        deliveries.c.status,
        deliveries.c.chat_id,
    )
    Index(f"{names[0]}_bot_status", jobs.c.bot_id, jobs.c.status)
    return BroadcastTables(metadata, jobs, deliveries, notifications, cooldowns)
