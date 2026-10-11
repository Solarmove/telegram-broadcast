import asyncio
from contextlib import asynccontextmanager

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase

from telegram_broadcast import BotBusy, BroadcastClient, RunPolicy, SentMessage

TOKEN = "123456:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi"


class LocalLock:
    """Shared test lock. Never a cross-process substitute for PostgreSQL."""

    def __init__(self):
        self.busy = set()

    @asynccontextmanager
    async def acquire(self, bot_id):
        if bot_id in self.busy:
            raise BotBusy("busy")
        self.busy.add(bot_id)
        try:
            yield
        finally:
            self.busy.remove(bot_id)


class FakeTransport:
    def __init__(self):
        self.sent = []
        self.reports = []
        self.outcomes = {}
        self.report_errors = {}
        self.started = asyncio.Event()
        self.release = None
        self.callback = None

    async def send(self, chat_id, message, file_id):
        self.sent.append((chat_id, file_id))
        self.started.set()
        if self.release is not None:
            await self.release.wait()
        if self.callback is not None:
            await self.callback()
        outcomes = self.outcomes.get(chat_id, [])
        if outcomes:
            outcome = outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        return SentMessage(chat_id + 100, "cached-media" if message.media_url else None)

    async def report(self, chat_id, text, message_id):
        self.reports.append((chat_id, text, message_id))
        errors = self.report_errors.get(chat_id, [])
        if errors:
            raise errors.pop(0)
        return message_id or abs(chat_id)


@pytest.fixture
async def harness():
    class Base(DeclarativeBase):
        pass

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    transport, lock = FakeTransport(), LocalLock()
    client = BroadcastClient(
        TOKEN,
        base=Base,
        engine=engine,
        transport=transport,
        lock=lock,
        policy=RunPolicy(
            send_interval=0,
            report_interval=999,
            network_retry_delay=0,
            control_poll_interval=0.001,
        ),
    )
    async with client:
        yield client, transport, engine, Base, lock
    await engine.dispose()
