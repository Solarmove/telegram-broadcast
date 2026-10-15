"""Build and persist progress reports independently of recipient delivery."""

import logging
from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta
from time import monotonic
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .errors import (
    DeliveryUncertain,
    PermanentSendError,
    RateLimited,
    RecipientUnavailable,
)
from .progress import ProgressMetrics, ProgressTracker
from .repository import Repository, StoredNotification
from .schema import utcnow
from .transport import BroadcastTransport
from .types import BroadcastResult, BroadcastStatus

logger = logging.getLogger(__name__)


class Reporter:
    def __init__(
        self,
        repository: Repository,
        sessions: async_sessionmaker[AsyncSession],
        transport: BroadcastTransport,
        sanitize_error: Callable[[Exception], str],
    ) -> None:
        self.repo = repository
        self.sessions = sessions
        self.transport = transport
        self.sanitize_error = sanitize_error
        self._progress = ProgressTracker()

    async def send(self, job_id: UUID, *, stop_error: str | None = None) -> None:
        async with self.sessions() as session:
            result = await self.repo.get_result(session, job_id)
            notifications = await self.repo.get_notifications(session, job_id)
            cooldown = await self.repo.get_cooldown(session)
        if cooldown is not None and cooldown > utcnow():
            return

        if stop_error is not None:
            result = replace(result, status=BroadcastStatus.FAILED, error=stop_error)

        metrics = self._progress.sample(
            result, now=utcnow(), monotonic_time=monotonic()
        )
        report_text = format_report(result, metrics)
        for notification in notifications:
            if notification.last_text == report_text and notification.error is None:
                continue
            if not await self._send_to_chat(job_id, notification, report_text):
                break  # A retry_after applies to the bot, including other admins.

    async def _send_to_chat(
        self, job_id: UUID, notification: StoredNotification, report_text: str
    ) -> bool:
        try:
            message_id = await self.transport.report(
                notification.chat_id,
                report_text,
                notification.message_id,
                notification.thread_id,
            )
        except RateLimited as exc:
            self._log_failure(job_id, notification, exc)
            async with self.sessions.begin() as session:
                retry_at = utcnow() + timedelta(seconds=exc.retry_after)
                await self.repo.extend_cooldown(session, retry_at)
                await self.repo.save_report(
                    session,
                    job_id,
                    notification.chat_id,
                    notification.thread_id,
                    message_id=notification.message_id,
                    text=notification.last_text,
                    error=self.sanitize_error(exc),
                )
            return False
        except (RecipientUnavailable, DeliveryUncertain, PermanentSendError) as exc:
            self._log_failure(job_id, notification, exc)
            async with self.sessions.begin() as session:
                await self.repo.save_report(
                    session,
                    job_id,
                    notification.chat_id,
                    notification.thread_id,
                    message_id=notification.message_id,
                    text=notification.last_text,
                    error=self.sanitize_error(exc),
                )
        else:
            async with self.sessions.begin() as session:
                await self.repo.save_report(
                    session,
                    job_id,
                    notification.chat_id,
                    notification.thread_id,
                    message_id=message_id,
                    text=report_text,
                    error=None,
                )
        return True

    def _log_failure(
        self, job_id: UUID, notification: StoredNotification, error: Exception
    ) -> None:
        logger.warning(
            "Broadcast %s: cannot send progress to chat_id=%s thread_id=%s: %s",
            job_id,
            notification.chat_id,
            notification.thread_id,
            self.sanitize_error(error),
        )


def _format_russian_report(result: BroadcastResult, metrics: ProgressMetrics) -> str:
    unknown = f"\n⚠️ Неизвестно: {result.unknown}" if result.unknown else ""
    if result.status.terminal:
        heading = {
            BroadcastStatus.COMPLETED: "✅ Рассылка завершена",
            BroadcastStatus.CANCELLED: "⛔ Рассылка отменена",
            BroadcastStatus.FAILED: "❌ Рассылка остановлена из-за ошибки",
        }[result.status]
        success_rate = (
            result.success / result.processed * 100 if result.processed else 0
        )
        return (
            f"{heading}\n\n"
            f"Успешно: {result.success}\nОшибок: {result.failed}{unknown}\n"
            f"Обработано: {result.processed}/{result.total}\n"
            f"Успешность: {success_rate:.1f}%\n\n"
            f"⏱ Время: {metrics.elapsed:.1f} сек\n"
            f"⚡ Средняя скорость: {metrics.average_speed:.2f} сообщений/сек"
        )
    heading = {
        BroadcastStatus.RUNNING: "Рассылка",
        BroadcastStatus.PAUSED: "⏸ Рассылка приостановлена",
        BroadcastStatus.QUEUED: "⏳ Рассылка ожидает продолжения",
    }[result.status]
    percentage = result.processed / result.total * 100 if result.total else 0
    eta = (
        f"~{metrics.remaining_seconds:.1f} сек"
        if metrics.remaining_seconds is not None
        else "рассчитывается"
    )
    return (
        f"{heading}\n\n"
        f"Обработано: {result.processed}/{result.total} ({percentage:.1f}%)\n"
        f"✅ Успешно: {result.success}\n❌ Ошибок: {result.failed}{unknown}\n\n"
        f"⚡ Средняя: {metrics.average_speed:.2f} msg/s\n"
        f"Текущая: {metrics.current_speed:.2f} msg/s\n\n"
        f"⏱ Прошло: {metrics.elapsed:.1f} сек\n⏳ Осталось: {eta}"
    )


_UK_REPORT_TEXT = {
    "Рассылка остановлена из-за ошибки": "Розсилку зупинено через помилку",
    "Рассылка ожидает продолжения": "Розсилка очікує продовження",
    "Рассылка приостановлена": "Розсилку призупинено",
    "Рассылка завершена": "Розсилку завершено",
    "Рассылка отменена": "Розсилку скасовано",
    "Рассылка": "Розсилка",
    "Средняя скорость": "Середня швидкість",
    "сообщений/сек": "повідомлень/с",
    "Неизвестно": "Невідомо",
    "Успешно": "Успішно",
    "Ошибок": "Помилок",
    "Обработано": "Оброблено",
    "Успешность": "Успішність",
    "Время": "Час",
    "Средняя": "Середня",
    "Текущая": "Поточна",
    "Прошло": "Минуло",
    "Осталось": "Залишилося",
    "рассчитывается": "обчислюється",
    "msg/s": "повідомлень/с",
    "сек": "с",
}


def format_report(result: BroadcastResult, metrics: ProgressMetrics) -> str:
    text = _format_russian_report(result, metrics)
    if result.notification_language == "uk":
        for source, translated in _UK_REPORT_TEXT.items():
            text = text.replace(source, translated)
    if result.error:
        label = "Причина помилки" if result.notification_language == "uk" else "Причина ошибки"
        text += f"\n\n⚠️ {label}: {result.error[:2000]}"
    return text
