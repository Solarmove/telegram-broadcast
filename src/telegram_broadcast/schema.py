"""Attach Core tables to the caller's Base without creating a global ORM registry."""

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import RLock

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
from sqlalchemy.orm import DeclarativeBase

from telegram_broadcast.errors import ConfigurationError

_REGISTRATION_LOCK = RLock()
_CACHE_KEY = "telegram_broadcast.registered_tables"


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


def register_tables(
    base: type[DeclarativeBase], *, prefix: str = "tg_broadcast"
) -> BroadcastTables:
    """Register once per metadata/prefix. No database I/O or application-table DDL."""
    metadata = getattr(base, "metadata", None)
    if not isinstance(metadata, MetaData):
        raise ConfigurationError("base must expose SQLAlchemy MetaData")
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,30}", prefix):
        raise ConfigurationError(
            "prefix must be a lowercase SQL identifier of at most 31 characters"
        )
    with _REGISTRATION_LOCK:
        cache = metadata.info.setdefault(_CACHE_KEY, {})
        if prefix in cache:
            return cache[prefix]
        names = [
            f"{prefix}_{suffix}"
            for suffix in ("jobs", "deliveries", "notifications", "cooldowns")
        ]
        if any(
            (f"{metadata.schema}.{name}" if metadata.schema else name)
            in metadata.tables
            for name in names
        ):
            raise ConfigurationError("Table names already exist; choose another prefix")
        tables = _build_tables(metadata, names)
        cache[prefix] = tables
        return tables


def _build_tables(metadata: MetaData, names: list[str]) -> BroadcastTables:
    jobs = Table(
        names[0],
        metadata,
        Column("id", Uuid, primary_key=True),
        Column("bot_id", BigInteger, nullable=False),
        Column("title", String(200), nullable=False),
        Column("text", Text),
        Column("media_url", Text),
        Column("media_type", String(8)),
        Column("parse_mode", String(16)),
        Column("media_file_id", Text),
        Column("status", String(16), nullable=False),
        Column("total", Integer, nullable=False),
        Column("success", Integer, nullable=False, default=0),
        Column("failed", Integer, nullable=False, default=0),
        Column("unknown", Integer, nullable=False, default=0),
        Column("error", Text),
        Column("next_retry_at", DateTime(timezone=True)),
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
