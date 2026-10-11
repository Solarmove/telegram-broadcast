"""OS-backed file locks serialize a bot's runs across local processes."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from filelock import FileLock, Timeout

from .errors import BotBusy


class FileBotLock:
    def __init__(self, database_path: Path) -> None:
        self.directory = database_path.with_name(database_path.name + ".locks")

    @asynccontextmanager
    async def acquire(self, bot_id: int) -> AsyncIterator[None]:
        self.directory.mkdir(parents=True, exist_ok=True)
        # A fresh object avoids FileLock's reentrant acquisition in this process.
        lock = FileLock(self.directory / f"{bot_id}.lock")
        try:
            lock.acquire(timeout=0)
        except Timeout:
            raise BotBusy(f"Bot {bot_id} already has an active runner") from None
        try:
            yield
        finally:
            lock.release()
