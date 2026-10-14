"""SQLite ownership, configuration and serialized schema initialization."""

import asyncio
import sqlite3
from pathlib import Path
from time import monotonic

from filelock import FileLock, Timeout
from sqlalchemy import URL, event
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    async_sessionmaker,
    create_async_engine,
)

from .errors import ConfigurationError
from .schema import build_tables

SCHEMA_VERSION = 3


class Database:
    def __init__(self, path: str | Path) -> None:
        if not str(path).strip() or str(path) == ":memory:" or "://" in str(path):
            raise ConfigurationError(
                "database_path must be a path to a persistent SQLite file"
            )
        self.path = Path(path).expanduser().resolve()
        if self.path.is_dir():
            raise ConfigurationError("database_path must name a file, not a directory")
        self.tables = build_tables()
        self.engine = create_async_engine(
            URL.create("sqlite+aiosqlite", database=str(self.path)),
            connect_args={"timeout": 30},
        )
        event.listen(self.engine.sync_engine, "connect", _configure_connection)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        self._initialization_lock = asyncio.Lock()
        self._initialized = False
        self._closed = False

    async def initialize(self) -> None:
        if self._closed:
            raise ConfigurationError("Client is closed; create a new BroadcastClient")
        async with self._initialization_lock:
            if self._initialized:
                return
            self.path.parent.mkdir(parents=True, exist_ok=True)
            schema_lock = await _acquire_schema_lock(
                self.path.with_name(self.path.name + ".schema.lock")
            )
            try:
                async with self.engine.begin() as connection:
                    await connection.exec_driver_sql("PRAGMA journal_mode=WAL")
                    result = await connection.exec_driver_sql("PRAGMA user_version")
                    version = result.scalar_one()
                    if version not in (0, 1, 2, SCHEMA_VERSION):
                        raise ConfigurationError(
                            f"Unsupported SQLite schema version {version}; "
                            f"this library supports version {SCHEMA_VERSION}"
                        )
                    if version == 1:
                        await _migrate_v1_to_v2(connection)
                    if version in (1, 2):
                        await _migrate_v2_to_v3(connection)
                    await connection.run_sync(self.tables.metadata.create_all)
                    if version < SCHEMA_VERSION:
                        await connection.exec_driver_sql(
                            f"PRAGMA user_version={SCHEMA_VERSION}"
                        )
                self._initialized = True
            finally:
                schema_lock.release()

    async def close(self) -> None:
        self._closed = True
        await self.engine.dispose()


async def _migrate_v1_to_v2(connection: AsyncConnection) -> None:
    jobs_name = "tg_broadcast_jobs"
    notification_name = "tg_broadcast_notifications"
    job_columns = await connection.exec_driver_sql(f'PRAGMA table_info("{jobs_name}")')
    existing_job_columns = {row[1] for row in job_columns.fetchall()}
    for name, type_ in (
        ("media_content_type", "VARCHAR(32)"),
        ("media_source_file_id", "TEXT"),
        ("media_path", "TEXT"),
    ):
        if name not in existing_job_columns:
            await connection.exec_driver_sql(
                f'ALTER TABLE "{jobs_name}" ADD COLUMN "{name}" {type_}'
            )
    if "media_type" in existing_job_columns:
        await connection.exec_driver_sql(
            f'UPDATE "{jobs_name}" SET media_content_type = media_type '
            "WHERE media_type IS NOT NULL"
        )

    legacy_name = f"{notification_name}_v1"
    await connection.exec_driver_sql(
        f'ALTER TABLE "{notification_name}" RENAME TO "{legacy_name}"'
    )
    await connection.exec_driver_sql(
        f'CREATE TABLE "{notification_name}" ('
        "broadcast_id CHAR(32) NOT NULL, "
        "chat_id BIGINT NOT NULL, "
        "thread_id BIGINT NOT NULL DEFAULT 0, "
        "message_id BIGINT, error TEXT, last_text TEXT, updated_at DATETIME, "
        f"PRIMARY KEY (broadcast_id, chat_id, thread_id), "
        f'FOREIGN KEY(broadcast_id) REFERENCES "{jobs_name}" (id) ON DELETE CASCADE)'
    )
    await connection.exec_driver_sql(
        f'INSERT INTO "{notification_name}" '
        "(broadcast_id, chat_id, thread_id, message_id, error, last_text, updated_at) "
        f"SELECT broadcast_id, chat_id, 0, message_id, error, last_text, updated_at "
        f'FROM "{legacy_name}"'
    )
    await connection.exec_driver_sql(f'DROP TABLE "{legacy_name}"')


async def _migrate_v2_to_v3(connection: AsyncConnection) -> None:
    columns = await connection.exec_driver_sql('PRAGMA table_info("tg_broadcast_jobs")')
    existing = {row[1] for row in columns}
    for name in ("started_at", "finished_at"):
        if name not in existing:
            await connection.exec_driver_sql(
                f'ALTER TABLE tg_broadcast_jobs ADD COLUMN "{name}" DATETIME'
            )


def _configure_connection(connection: sqlite3.Connection, _record: object) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.execute("PRAGMA synchronous=FULL")
    finally:
        cursor.close()


async def _acquire_schema_lock(path: Path) -> FileLock:
    lock = FileLock(path)
    deadline = monotonic() + 30
    while True:
        try:
            lock.acquire(timeout=0)
            return lock
        except Timeout:
            if monotonic() >= deadline:
                raise ConfigurationError(
                    "Timed out waiting for SQLite schema initialization"
                ) from None
            await asyncio.sleep(0.05)
