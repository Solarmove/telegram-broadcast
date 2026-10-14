import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from telegram_broadcast import (
    BroadcastMessage,
    BroadcastResult,
    BroadcastStatus,
    RateLimited,
    RecipientUnavailable,
    RunPolicy,
)
from telegram_broadcast.progress import ProgressMetrics, ProgressTracker
from telegram_broadcast.reporting import format_report


def result(**changes):
    started = datetime(2026, 10, 14, tzinfo=UTC)
    return replace(
        BroadcastResult(
            id=uuid4(),
            bot_id=1,
            title="test",
            status=BroadcastStatus.RUNNING,
            total=3323,
            success=871,
            failed=484,
            unknown=0,
            error=None,
            next_retry_at=None,
            created_at=started - timedelta(days=1),
            updated_at=started,
            started_at=started,
        ),
        **changes,
    )


def test_progress_matches_requested_format_and_rates():
    job = result()
    assert job.started_at is not None
    tracker = ProgressTracker()
    tracker.sample(
        replace(job, success=774),
        now=job.started_at + timedelta(seconds=252.3),
        monotonic_time=252.3,
    )
    metrics = tracker.sample(
        job,
        now=job.started_at + timedelta(seconds=272.3),
        monotonic_time=272.3,
    )
    assert format_report(job, metrics) == (
        "Рассылка\n\n"
        "Обработано: 1355/3323 (40.8%)\n"
        "✅ Успешно: 871\n❌ Ошибок: 484\n\n"
        "⚡ Средняя: 4.98 msg/s\nТекущая: 4.85 msg/s\n\n"
        "⏱ Прошло: 272.3 сек\n⏳ Осталось: ~395.5 сек"
    )


def test_final_format_and_frozen_duration_after_restart():
    job = result(success=950, failed=539, status=BroadcastStatus.COMPLETED)
    assert job.started_at is not None
    job = replace(job, finished_at=job.started_at + timedelta(seconds=299.9))
    metrics = ProgressTracker().sample(
        job,
        now=job.started_at + timedelta(days=1),
        monotonic_time=100,
    )
    assert format_report(job, metrics) == (
        "✅ Рассылка завершена\n\n"
        "Успешно: 950\nОшибок: 539\nОбработано: 1489/3323\n"
        "Успешность: 63.8%\n\n"
        "⏱ Время: 299.9 сек\n⚡ Средняя скорость: 4.96 сообщений/сек"
    )


def test_no_division_by_zero_or_misleading_success_for_unknown_deliveries():
    job = result(total=0, success=0, failed=0, started_at=None)
    metrics = ProgressTracker().sample(job, now=job.created_at, monotonic_time=0)
    assert metrics.average_speed == 0
    assert "(0.0%)" in format_report(job, metrics)
    job = result(unknown=2, status=BroadcastStatus.CANCELLED)
    report = format_report(job, ProgressMetrics(10, 1, 1, 10))
    assert "Рассылка отменена" in report
    assert "⚠️ Неизвестно: 2" in report
    assert "Рассылка завершена" not in report


async def test_single_recipient_sends_initial_report_and_edits_it_to_final(harness):
    client, transport, _ = harness
    job = await client.create(BroadcastMessage("test"), [1], notification_chats=[-10])
    finished = await client.run(job)
    assert len(transport.reports) == 2
    initial, final = transport.reports
    assert initial[0] == final[0] == -10
    assert initial[2] is None
    assert final[2] == finished.notifications[0].message_id == 10
    assert "Обработано: 0/1 (0.0%)" in initial[1]
    assert "✅ Рассылка завершена" in final[1]
    assert "Обработано: 1/1" in final[1]
    assert finished.started_at is not None and finished.finished_at is not None
    await client.run(job)
    assert len(transport.reports) == 2


async def test_progress_updates_even_while_delivery_is_waiting(harness, monkeypatch):
    client, transport, _ = harness
    client._policy = RunPolicy(send_interval=0, report_interval=0.02)
    # The polling interval also bounds report frequency.
    client._policy = replace(client._policy, control_poll_interval=0.01)
    transport.release = asyncio.Event()
    updated = asyncio.Event()
    original = transport.report

    async def report(*args):
        message_id = await original(*args)
        if args[2] is not None:
            updated.set()
        return message_id

    monkeypatch.setattr(transport, "report", report)
    job = await client.create(BroadcastMessage("test"), [1], notification_chats=[-10])
    task = asyncio.create_task(client.run(job))
    try:
        await asyncio.wait_for(transport.started.wait(), timeout=5)
        await asyncio.wait_for(updated.wait(), timeout=5)
        assert not task.done()
        assert "Обработано: 0/1" in transport.reports[-1][1]
    finally:
        transport.release.set()
        await asyncio.wait_for(task, timeout=5)
    assert "✅ Рассылка завершена" in transport.reports[-1][1]


async def test_notification_failure_is_visible_in_logs_and_results(harness, caplog):
    client, transport, _ = harness
    transport.report_errors[-10] = [RecipientUnavailable("chat not found")] * 2
    job = await client.create(BroadcastMessage("test"), [1], notification_chats=[-10])
    finished = await client.run(job)
    assert finished.success == 1
    assert finished.notifications[0].error == "chat not found"
    assert "chat_id=-10" in caplog.text
    assert "chat not found" in caplog.text


async def test_report_rate_limit_stops_requests_to_other_admins(harness):
    client, transport, _ = harness
    transport.report_errors[-20] = [RateLimited(60)]
    job = await client.create(
        BroadcastMessage("test"), [1], notification_chats=[-20, -10]
    )
    await client.run(job, wait_for_cooldown=False)
    assert [report[0] for report in transport.reports] == [-20]
    assert not transport.sent
