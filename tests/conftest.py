import asyncio

import pytest

from telegram_broadcast import BroadcastClient, RunPolicy, SentMessage

TOKEN = "123456:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi"


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
        return SentMessage(chat_id + 100, "cached-media" if message.media else None)

    async def report(self, chat_id, text, message_id, thread_id=None):
        self.reports.append((chat_id, text, message_id, thread_id))
        errors = self.report_errors.get(chat_id, [])
        if errors:
            raise errors.pop(0)
        return message_id or abs(chat_id)


@pytest.fixture
async def harness(tmp_path):
    transport = FakeTransport()
    client = BroadcastClient(
        TOKEN,
        database_path=tmp_path / "broadcasts.sqlite3",
        transport=transport,
        policy=RunPolicy(
            send_interval=0,
            report_interval=999,
            network_retry_delay=0,
            control_poll_interval=0.001,
        ),
    )
    async with client:
        yield client, transport, client._database.engine
