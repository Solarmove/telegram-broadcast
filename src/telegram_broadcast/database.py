"""SQLite ownership, configuration and serialized schema initialization."""

import asyncio
import sqlite3
from pathlib import Path
from time import monotonic

from filelock import FileLock, Timeout
from sqlalchemy import URL, event
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from .errors import ConfigurationError
from .schema import build_tables

SCHEMA_VERSION = 1


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
                    if version not in (0, SCHEMA_VERSION):
                        raise ConfigurationError(
                            f"Unsupported SQLite schema version {version}; "
                            f"this library supports version {SCHEMA_VERSION}"
                        )
                    await connection.run_sync(self.tables.metadata.create_all)
                    if version == 0:
                        await connection.exec_driver_sql(
                            f"PRAGMA user_version={SCHEMA_VERSION}"
                        )
                self._initialized = True
            finally:
                schema_lock.release()

    async def close(self) -> None:
        self._closed = True
        await self.engine.dispose()


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
