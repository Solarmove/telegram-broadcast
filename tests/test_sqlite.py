"""Integration tests use actual SQLite files and the production file lock."""

import asyncio
import multiprocessing
import os
from pathlib import Path

import pytest
from conftest import TOKEN, FakeTransport
from sqlalchemy import text, update

from telegram_broadcast import (
    BotBusy,
    BroadcastClient,
    BroadcastMessage,
    ConfigurationError,
    RateLimited,
    RunPolicy,
)
from telegram_broadcast.locking import FileBotLock


async def test_reopen_preserves_results_and_prevents_duplicate_delivery(tmp_path):
    path = tmp_path / "nested" / "broadcasts.sqlite3"
    first_transport = FakeTransport()
    async with BroadcastClient(
        TOKEN, database_path=path, transport=first_transport
    ) as first:
        job = await first.create(
            BroadcastMessage("Hello"), [1, 2], notification_chat_ids=[-10]
        )
        assert (await first.run(job)).success == 2
    second_transport = FakeTransport()
    async with BroadcastClient(
        TOKEN, database_path=path, transport=second_transport
    ) as second:
        result = await second.run(job)
        assert result.success == 2
        assert len(result.notifications) == 1
        assert len(await second.get_deliveries(job)) == 2
    assert not second_transport.sent and not second_transport.reports


async def test_crash_journal_recovers_after_reopen(tmp_path):
    path = tmp_path / "broadcasts.sqlite3"
    async with BroadcastClient(
        TOKEN, database_path=path, transport=FakeTransport()
    ) as first:
        job = await first.create(BroadcastMessage("Hello"), [1, 2])
        async with first._database.engine.begin() as connection:
            await connection.execute(
                update(first.tables.deliveries)
                .where(first.tables.deliveries.c.chat_id == 1)
                .values(status="sending")
            )
    transport = FakeTransport()
    async with BroadcastClient(
        TOKEN, database_path=path, transport=transport
    ) as second:
        result = await second.run(job)
        assert result.unknown == 1 and result.success == 1
        assert [chat for chat, _ in transport.sent] == [2]


async def test_clients_share_lock_but_allow_results_and_other_bots(harness):
    first, transport, _ = harness
    transport.release = asyncio.Event()
    job = await first.create(BroadcastMessage("Hello"), [1])
    async with BroadcastClient(
        TOKEN, database_path=first.database_path, transport=FakeTransport()
    ) as second:
        task = asyncio.create_task(first.run(job))
        await asyncio.wait_for(transport.started.wait(), 5)
        try:
            assert (await second.get_result(job)).total == 1
            with pytest.raises(BotBusy):
                await second.run(job)
            await second.create(BroadcastMessage("Other broadcast"), [2])
            async with BroadcastClient(
                TOKEN.replace("123456", "987654"),
                database_path=first.database_path,
                transport=FakeTransport(),
            ) as other_bot:
                other_job = await other_bot.create(BroadcastMessage("Other bot"), [3])
                assert (await other_bot.run(other_job)).success == 1
        finally:
            transport.release.set()
            await task
        assert (await second.run(job)).success == 1


async def test_cooldown_survives_reopen_and_applies_to_other_broadcast(tmp_path):
    path = tmp_path / "broadcasts.sqlite3"
    transport = FakeTransport()
    transport.outcomes[1] = [RateLimited(60)]
    async with BroadcastClient(
        TOKEN,
        database_path=path,
        transport=transport,
        policy=RunPolicy(send_interval=0),
    ) as first:
        job = await first.create(BroadcastMessage("Hello"), [1])
        await first.run(job, wait_for_cooldown=False)
    transport = FakeTransport()
    async with BroadcastClient(
        TOKEN, database_path=path, transport=transport
    ) as second:
        other_job = await second.create(BroadcastMessage("Other"), [2])
        result = await second.run(other_job, wait_for_cooldown=False)
        assert result.next_retry_at is not None and result.processed == 0
    assert not transport.sent


async def test_connection_pragmas_and_foreign_keys(harness):
    client, _, engine = harness
    async with engine.connect() as connection:
        assert await connection.scalar(text("PRAGMA journal_mode")) == "wal"
        assert await connection.scalar(text("PRAGMA foreign_keys")) == 1
        assert await connection.scalar(text("PRAGMA busy_timeout")) == 30000
        assert await connection.scalar(text("PRAGMA synchronous")) == 2
    job = await client.create(
        BroadcastMessage("Hello"), [1], notification_chat_ids=[-10]
    )
    async with engine.begin() as connection:
        await connection.execute(
            client.tables.broadcasts.delete().where(
                client.tables.broadcasts.c.id == job
            )
        )
    async with engine.connect() as connection:
        assert (await connection.execute(client.tables.deliveries.select())).all() == []
        assert (
            await connection.execute(client.tables.notifications.select())
        ).all() == []


async def test_lazy_initialization_and_closed_client(tmp_path):
    client = BroadcastClient(
        TOKEN, database_path=tmp_path / "broadcasts.sqlite3", transport=FakeTransport()
    )
    job = await client.create(BroadcastMessage("Hello"), [1])
    assert (await client.run(job)).success == 1
    await client.close()
    with pytest.raises(ConfigurationError, match="closed"):
        await client.get_result(job)


async def test_simultaneous_schema_initialization(tmp_path):
    path = tmp_path / "new.sqlite3"
    first = BroadcastClient(TOKEN, database_path=path, transport=FakeTransport())
    second = BroadcastClient(TOKEN, database_path=path, transport=FakeTransport())
    try:
        ids = await asyncio.gather(
            first.create(BroadcastMessage("First"), [1]),
            second.create(BroadcastMessage("Second"), [2]),
        )
        assert ids[0] != ids[1]
        assert (await first.get_result(ids[1])).total == 1
    finally:
        await first.close()
        await second.close()


@pytest.mark.parametrize("path", ["", ":memory:", "postgresql://localhost/database"])
def test_invalid_database_path(path):
    with pytest.raises(ConfigurationError):
        BroadcastClient(TOKEN, database_path=path, transport=FakeTransport())


def _hold_lock_and_crash(path, ready, release):
    async def hold():
        async with FileBotLock(Path(path)).acquire(123456):
            ready.set()
            release.wait(15)
            os._exit(0)

    asyncio.run(hold())


async def test_process_lock_is_released_after_crash(tmp_path):
    path = tmp_path / "broadcasts.sqlite3"
    context = multiprocessing.get_context("spawn")
    ready, release = context.Event(), context.Event()
    process = context.Process(
        target=_hold_lock_and_crash, args=(str(path), ready, release)
    )
    process.start()
    try:
        assert await asyncio.to_thread(ready.wait, 10)
        with pytest.raises(BotBusy):
            async with FileBotLock(path).acquire(123456):
                pass
        release.set()
        await asyncio.to_thread(process.join, 10)
        assert process.exitcode == 0
        async with FileBotLock(path).acquire(123456):
            pass
    finally:
        if process.is_alive():
            process.terminate()
            await asyncio.to_thread(process.join, 5)
        process.close()
