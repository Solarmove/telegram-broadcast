"""Requires an isolated PostgreSQL database; all test tables have unique prefixes."""

import asyncio
import os
from uuid import uuid4

import pytest
from conftest import TOKEN, FakeTransport
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase

from telegram_broadcast import (
    BotBusy,
    BroadcastClient,
    BroadcastMessage,
    RateLimited,
    RunPolicy,
)

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(
        not os.getenv("TELEGRAM_BROADCAST_TEST_DSN"),
        reason="PostgreSQL test DSN is not configured",
    ),
]


@pytest.fixture
async def postgres():
    class Base(DeclarativeBase):
        pass

    engine = create_async_engine(os.environ["TELEGRAM_BROADCAST_TEST_DSN"])
    transport = FakeTransport()
    client = BroadcastClient(
        TOKEN,
        base=Base,
        engine=engine,
        prefix="test_broadcast_" + uuid4().hex[:12],
        transport=transport,
        policy=RunPolicy(send_interval=0),
    )
    async with client:
        yield client, transport, engine, Base
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync: Base.metadata.drop_all(sync, tables=list(client.tables.all))
        )
    await engine.dispose()


async def test_postgres_journal_and_results(postgres):
    client, _, _, _ = postgres
    job = await client.create(
        BroadcastMessage("hello"), [1, 2], notification_chat_ids=[-10, -20]
    )
    result = await client.run(job)
    assert result.success == 2
    assert len(result.notifications) == 2
    assert len(await client.get_deliveries(job)) == 2


async def test_postgres_lock_survives_journal_commits(postgres):
    client, transport, engine, base = postgres
    transport.release = asyncio.Event()
    job = await client.create(BroadcastMessage("hello"), [1])
    task = asyncio.create_task(client.run(job))
    await asyncio.wait_for(transport.started.wait(), 5)
    other = BroadcastClient(
        TOKEN,
        base=base,
        engine=engine,
        prefix=client.tables.broadcasts.name.removesuffix("_jobs"),
        transport=FakeTransport(),
    )
    try:
        assert (await other.get_result(job)).total == 1
        with pytest.raises(BotBusy):
            await other.run(job)
    finally:
        transport.release.set()
        await task
    # Lock is released when run finishes.
    assert (await other.run(job)).success == 1


async def test_postgres_cooldown_is_shared_between_broadcasts(postgres):
    client, transport, _, _ = postgres
    transport.outcomes[1] = [RateLimited(60)]
    first = await client.create(BroadcastMessage("first"), [1])
    second = await client.create(BroadcastMessage("second"), [2])
    await client.run(first, wait_for_cooldown=False)
    result = await client.run(second, wait_for_cooldown=False)
    assert result.processed == 0 and result.next_retry_at is not None
    assert transport.sent == [(1, None)]
