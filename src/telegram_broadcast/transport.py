"""Telegram adapter; network failures are translated into domain errors."""

from typing import Protocol

from aiogram import Bot
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramNotFound,
    TelegramRetryAfter,
    TelegramServerError,
    TelegramUnauthorizedError,
)

from .errors import (
    DeliveryUncertain,
    PermanentSendError,
    RateLimited,
    RecipientUnavailable,
)
from .types import BroadcastMessage, MediaType, SentMessage


class BroadcastTransport(Protocol):
    async def send(
        self, chat_id: int, message: BroadcastMessage, file_id: str | None
    ) -> SentMessage: ...
    async def report(self, chat_id: int, text: str, message_id: int | None) -> int: ...


class AiogramTransport:
    def __init__(self, token: str) -> None:
        self._bot = Bot(token)

    async def close(self) -> None:
        await self._bot.session.close()

    async def send(
        self, chat_id: int, message: BroadcastMessage, file_id: str | None
    ) -> SentMessage:
        try:
            if message.media_type == MediaType.PHOTO:
                result = await self._bot.send_photo(
                    chat_id,
                    photo=file_id or message.media_url or "",
                    caption=message.text,
                    parse_mode=message.parse_mode,
                )
                return SentMessage(
                    result.message_id,
                    result.photo[-1].file_id if result.photo else None,
                )
            if message.media_type == MediaType.VIDEO:
                result = await self._bot.send_video(
                    chat_id,
                    video=file_id or message.media_url or "",
                    caption=message.text,
                    parse_mode=message.parse_mode,
                )
                return SentMessage(
                    result.message_id, result.video.file_id if result.video else None
                )
            result = await self._bot.send_message(
                chat_id, message.text or "", parse_mode=message.parse_mode
            )
            return SentMessage(result.message_id)
        except Exception as exc:
            raise _translate(exc) from exc

    async def report(self, chat_id: int, text: str, message_id: int | None) -> int:
        try:
            if message_id is None:
                result = await self._bot.send_message(chat_id, text, parse_mode=None)
                return result.message_id
            await self._bot.edit_message_text(
                text, chat_id=chat_id, message_id=message_id, parse_mode=None
            )
            return message_id
        except TelegramBadRequest as exc:
            if (
                "message is not modified" in exc.message.lower()
                and message_id is not None
            ):
                return message_id
            if (
                message_id is not None
                and "message to edit not found" in exc.message.lower()
            ):
                return await self.report(chat_id, text, None)
            raise _translate(exc) from exc
        except Exception as exc:
            raise _translate(exc) from exc


def _translate(exc: Exception) -> Exception:
    if isinstance(exc, TelegramRetryAfter):
        return RateLimited(exc.retry_after)
    if isinstance(exc, (TelegramForbiddenError, TelegramNotFound)):
        return RecipientUnavailable(str(exc))
    if isinstance(exc, (TelegramNetworkError, TelegramServerError)):
        return DeliveryUncertain(str(exc))
    if isinstance(exc, TelegramBadRequest):
        if any(
            part in exc.message.lower()
            for part in ("chat not found", "user is deactivated", "bot was blocked")
        ):
            return RecipientUnavailable(str(exc))
        return PermanentSendError(str(exc))
    if isinstance(exc, TelegramUnauthorizedError):
        return PermanentSendError(str(exc))
    return exc
