import asyncio

import pytest
from conftest import TOKEN
from sqlalchemy import inspect, select, update

from telegram_broadcast import (
    BotBusy,
    BroadcastClient,
    BroadcastMessage,
    BroadcastStatus,
    DeliveryStatus,
    DeliveryUncertain,
    InvalidTransition,
    MediaType,
    PermanentSendError,
    RateLimited,
    RecipientUnavailable,
    WrongBot,
)


async def test_success_deduplication_reports_and_idempotent_run(harness):
    client, transport, *_ = harness
    job = await client.create(
        BroadcastMessage("Hello"), [3, 1, 3, 2], notification_chat_ids=[-10, -20, -10]
    )
    result = await client.run(job)
    assert result.status == BroadcastStatus.COMPLETED
    assert (result.total, result.success, result.remaining) == (3, 3, 0)
    assert len(result.notifications) == 2
    assert [s[0] for s in transport.sent] == [1, 2, 3]
    assert len(transport.reports) == 4
    before = len(transport.reports)
    await client.run(job)
    assert len(transport.sent) == 3
    assert len(transport.reports) == before
    deliveries = await client.get_deliveries(job, limit=1, offset=1)
    assert deliveries[0].chat_id == 2
    assert deliveries[0].status == DeliveryStatus.SUCCESS


async def test_owned_tables_are_created_idempotently(harness):
    client, _, engine = harness
    await client._database.initialize()
    async with engine.connect() as conn:
        names = await conn.run_sync(lambda sync: inspect(sync).get_table_names())
    assert set(names) == {
        "tg_broadcast_jobs",
        "tg_broadcast_deliveries",
        "tg_broadcast_notifications",
        "tg_broadcast_cooldowns",
    }


async def test_recipient_failure_does_not_stop_others(harness):
    client, transport, *_ = harness
    transport.outcomes[1] = [RecipientUnavailable("blocked")]
    job = await client.create(BroadcastMessage("Hello"), [1, 2])
    result = await client.run(job)
    assert (result.failed, result.success, result.unknown) == (1, 1, 0)
    assert result.status == BroadcastStatus.COMPLETED


async def test_uncertain_delivery_stops_and_is_never_resent(harness):
    client, transport, *_ = harness
    transport.outcomes[1] = [DeliveryUncertain("timeout")]
    job = await client.create(BroadcastMessage("Hello"), [1, 2, 3])
    result = await client.run(job)
    assert (result.status, result.unknown, result.remaining) == (
        BroadcastStatus.QUEUED,
        1,
        2,
    )
    assert len(transport.sent) == 1
    result = await client.run(job)
    assert result.success == 2
    assert result.unknown == 1
    assert [s[0] for s in transport.sent] == [1, 2, 3]


async def test_cancellation_recovery_preserves_uncertainty(harness):
    client, transport, *_ = harness
    transport.release = asyncio.Event()
    job = await client.create(BroadcastMessage("Hello"), [1, 2])
    task = asyncio.create_task(client.run(job))
    await transport.started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    transport.release = None
    result = await client.run(job)
    assert (result.success, result.unknown) == (1, 1)
    assert [s[0] for s in transport.sent] == [1, 2]


async def test_permanent_failure_stops_broadcast_and_redacts_token(harness):
    client, transport, engine, *_ = harness
    transport.outcomes[1] = [PermanentSendError(f"invalid token {TOKEN}")]
    job = await client.create(BroadcastMessage("Hello"), [1, 2])
    result = await client.run(job)
    assert result.status == BroadcastStatus.FAILED
    assert result.remaining == 1
    assert TOKEN not in result.error
    async with engine.connect() as conn:
        rows = (await conn.execute(select(client.tables.broadcasts))).mappings().all()
    assert TOKEN not in str(rows)


async def test_rate_limit_returns_retry_time_and_preserves_pending(harness):
    client, transport, engine, *_ = harness
    transport.outcomes[1] = [RateLimited(60)]
    job = await client.create(BroadcastMessage("Hello"), [1, 2])
    result = await client.run(job, wait_for_cooldown=False)
    assert result.status == BroadcastStatus.QUEUED
    assert result.processed == 0 and result.next_retry_at is not None
    assert (await client.get_deliveries(job))[0].status == DeliveryStatus.PENDING
    async with engine.begin() as conn:
        await conn.execute(client.tables.cooldowns.delete())
    result = await client.run(job)
    assert result.success == 2


async def test_notification_failure_is_isolated_and_retryable(harness):
    client, transport, *_ = harness
    transport.report_errors[-10] = [
        RecipientUnavailable("blocked"),
        RecipientUnavailable("blocked"),
    ]
    job = await client.create(
        BroadcastMessage("Hello"), [1], notification_chat_ids=[-10, -20]
    )
    result = await client.run(job)
    assert result.success == 1
    assert next(n for n in result.notifications if n.chat_id == -10).error == "blocked"
    await client.run(job)
    result = await client.get_result(job)
    assert all(n.error is None for n in result.notifications)
    assert len(transport.sent) == 1


async def test_notification_rate_limit_blocks_delivery(harness):
    client, transport, *_ = harness
    transport.report_errors[-10] = [RateLimited(60)]
    job = await client.create(
        BroadcastMessage("Hello"), [1], notification_chat_ids=[-10]
    )
    result = await client.run(job, wait_for_cooldown=False)
    assert result.processed == 0
    assert not transport.sent


async def test_pause_resume_cancel_and_invalid_transition(harness):
    client, transport, *_ = harness
    job = await client.create(BroadcastMessage("Hello"), [1, 2])
    await client.pause(job)
    assert (await client.run(job)).status == BroadcastStatus.PAUSED
    assert not transport.sent
    await client.resume(job)
    await client.cancel(job)
    assert (await client.run(job)).status == BroadcastStatus.CANCELLED
    with pytest.raises(InvalidTransition):
        await client.resume(job)


async def test_pause_during_delivery_finishes_only_inflight(harness):
    client, transport, *_ = harness
    job = await client.create(BroadcastMessage("Hello"), [1, 2])
    transport.callback = lambda: client.pause(job)
    result = await client.run(job)
    assert result.status == BroadcastStatus.PAUSED and result.success == 1
    transport.callback = None
    await client.resume(job)
    assert (await client.run(job)).success == 2


async def test_busy_lock(harness):
    client, transport, *_ = harness
    transport.release = asyncio.Event()
    job = await client.create(BroadcastMessage("Hello"), [1])
    task = asyncio.create_task(client.run(job))
    await transport.started.wait()
    with pytest.raises(BotBusy):
        await client.run(job)
    transport.release.set()
    await task


async def test_wrong_bot_cannot_read_or_control(harness):
    client, transport, _ = harness
    job = await client.create(BroadcastMessage("Hello"), [1])
    other = BroadcastClient(
        TOKEN.replace("123456", "987654"),
        database_path=client.database_path,
        transport=transport,
    )
    async with other:
        with pytest.raises(WrongBot):
            await other.get_result(job)
        with pytest.raises(WrongBot):
            await other.cancel(job)
        with pytest.raises(WrongBot):
            await other.run(job)


async def test_media_file_id_reuse(harness):
    client, transport, *_ = harness
    job = await client.create(
        BroadcastMessage("Caption", "https://example.org/photo.jpg", MediaType.PHOTO),
        [1, 2],
    )
    await client.run(job)
    assert transport.sent == [(1, None), (2, "cached-media")]


@pytest.mark.parametrize("ids", [[], [0], [1, True], [2**63], ["1"]])
async def test_invalid_ids_do_not_create_jobs(harness, ids):
    client, *_ = harness
    with pytest.raises(ValueError):
        await client.create(BroadcastMessage("Hello"), ids)


async def test_unexpected_error_propagates_and_journal_recovers(harness):
    client, transport, *_ = harness
    transport.outcomes[1] = [RuntimeError("programming bug")]
    job = await client.create(BroadcastMessage("Hello"), [1, 2])
    with pytest.raises(RuntimeError):
        await client.run(job)
    assert (await client.get_deliveries(job))[0].status == DeliveryStatus.SENDING
    result = await client.run(job)
    assert result.success == 1 and result.unknown == 1


async def test_recover_existing_journal(harness):
    client, _, engine, *_ = harness
    job = await client.create(BroadcastMessage("Hello"), [1, 2])
    async with engine.begin() as conn:
        await conn.execute(
            update(client.tables.deliveries)
            .where(client.tables.deliveries.c.chat_id == 1)
            .values(status="sending")
        )
    result = await client.run(job)
    assert result.unknown == 1 and result.success == 1


async def test_commit_failure_does_not_publish_success(harness, monkeypatch):
    client, _, *_ = harness
    job = await client.create(BroadcastMessage("Hello"), [1, 2])
    original = client._repository.delivery

    async def failing_delivery(*args, **kwargs):
        await original(*args, **kwargs)
        if args[3] == DeliveryStatus.SUCCESS:
            raise RuntimeError("database transaction failed")

    monkeypatch.setattr(client._repository, "delivery", failing_delivery)
    with pytest.raises(RuntimeError):
        await client.run(job)
    assert (await client.get_result(job)).success == 0
    assert (await client.get_deliveries(job))[0].status == DeliveryStatus.SENDING
    monkeypatch.setattr(client._repository, "delivery", original)
    result = await client.run(job)
    assert result.success == 1 and result.unknown == 1


async def test_wait_for_cooldown_retries_only_rejected_request(harness):
    client, transport, *_ = harness
    transport.outcomes[1] = [RateLimited(0.005)]
    job = await client.create(BroadcastMessage("Hello"), [1, 2])
    result = await client.run(job)
    assert result.success == 2
    assert [s[0] for s in transport.sent] == [1, 1, 2]


async def test_max_duration_bounds_cooldown_wait(harness):
    client, transport, *_ = harness
    transport.outcomes[1] = [RateLimited(60)]
    job = await client.create(BroadcastMessage("Hello"), [1])
    # Establish the cooldown before measuring a bounded run: slow SQLite I/O
    # can otherwise consume the entire budget before Telegram is contacted.
    initial = await client.run(job, wait_for_cooldown=False)
    assert initial.next_retry_at is not None

    result = await asyncio.wait_for(client.run(job, max_duration=0.02), timeout=2)
    assert result.status == BroadcastStatus.QUEUED
    assert result.success == 0 and result.next_retry_at == initial.next_retry_at
    assert transport.sent == [(1, None)]
    assert (await client.get_deliveries(job))[0].status == DeliveryStatus.PENDING


async def test_duration_expired_during_report_does_not_mark_delivery_uncertain(
    harness, monkeypatch
):
    client, transport, *_ = harness
    original = transport.report

    async def slow_report(*args):
        await asyncio.sleep(0.03)
        return await original(*args)

    monkeypatch.setattr(transport, "report", slow_report)
    job = await client.create(
        BroadcastMessage("Hello"), [1], notification_chat_ids=[-10]
    )
    result = await client.run(job, max_duration=0.01)
    assert result.status == BroadcastStatus.QUEUED
    assert not transport.sent
    assert (await client.get_deliveries(job))[0].status == DeliveryStatus.PENDING
