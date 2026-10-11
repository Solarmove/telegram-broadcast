from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from telegram_broadcast.errors import BotBusy, ConfigurationError


class BotLock(Protocol):
    def acquire(self, bot_id: int) -> AbstractAsyncContextManager[None]: ...


class PostgresBotLock:
    def __init__(self, engine: AsyncEngine):
        if engine.dialect.name != "postgresql":
            raise ConfigurationError(
                "Running requires PostgreSQL; inject a local lock in SQLite tests"
            )
        self.engine = engine

    @asynccontextmanager
    async def acquire(self, bot_id: int) -> AsyncIterator[None]:
        # Hold this independently of the delivery journal's commits.
        async with self.engine.begin() as connection:
            acquired = await connection.scalar(
                text("SELECT pg_try_advisory_xact_lock(:bot_id)"), {"bot_id": bot_id}
            )
            if not acquired:
                raise BotBusy(f"Bot {bot_id} already has an active runner")
            yield
