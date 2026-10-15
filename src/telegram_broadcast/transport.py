"""Telegram adapter; network failures are translated into domain errors."""

from collections.abc import Sequence
from typing import Protocol

from aiogram import Bot
from aiogram.enums import ContentType
from aiogram.exceptions import (
    TelegramAPIError,
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramNotFound,
    TelegramRetryAfter,
    TelegramServerError,
    TelegramUnauthorizedError,
)
from aiogram.types import FSInputFile

from .errors import (
    DeliveryUncertain,
    PermanentSendError,
    RateLimited,
    RecipientUnavailable,
)
from .types import BroadcastMessage, NotificationRecipient, SentMessage
from .validation import validate_configuration


class BroadcastTransport(Protocol):
    async def validate(
        self, notifications: Sequence[NotificationRecipient]
    ) -> None: ...

    async def send(
        self, chat_id: int, message: BroadcastMessage, file_id: str | None
    ) -> SentMessage: ...
    async def report(
        self,
        chat_id: int,
        text: str,
        message_id: int | None,
        thread_id: int | None = None,
    ) -> int: ...


class AiogramTransport:
    def __init__(self, token: str) -> None:
        self._bot = Bot(token)

    async def close(self) -> None:
        await self._bot.session.close()

    async def validate(self, notifications: Sequence[NotificationRecipient]) -> None:
        await validate_configuration(self._bot, notifications)

    async def send(
        self, chat_id: int, message: BroadcastMessage, file_id: str | None
    ) -> SentMessage:
        try:
            if message.media is not None:
                media = message.media
                method_name, parameter, result_attribute = _MEDIA_METHODS[
                    media.content_type
                ]
                source = file_id or media.file_id or media.url or media.path_to_file
                if media.path_to_file is not None and file_id is None:
                    source = FSInputFile(media.path_to_file)
                kwargs: dict[str, object] = {parameter: source}
                if media.content_type not in (
                    ContentType.STICKER,
                    ContentType.VIDEO_NOTE,
                ):
                    kwargs.update(caption=message.text, parse_mode=message.parse_mode)
                result = await getattr(self._bot, method_name)(chat_id, **kwargs)
                sent_media = getattr(result, result_attribute, None)
                if media.content_type == ContentType.PHOTO and sent_media:
                    sent_media = sent_media[-1]
                return SentMessage(
                    result.message_id, getattr(sent_media, "file_id", None)
                )
            result = await self._bot.send_message(
                chat_id, message.text or "", parse_mode=message.parse_mode
            )
            return SentMessage(result.message_id)
        except Exception as exc:
            _raise_if_translated(exc)
            raise

    async def report(
        self,
        chat_id: int,
        text: str,
        message_id: int | None,
        thread_id: int | None = None,
    ) -> int:
        try:
            if message_id is None:
                result = await self._bot.send_message(
                    chat_id,
                    text,
                    parse_mode=None,
                    message_thread_id=thread_id,
                )
                return result.message_id
            await self._bot.edit_message_text(
                text,
                chat_id=chat_id,
                message_id=message_id,
                parse_mode=None,
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
                return await self.report(chat_id, text, None, thread_id)
            _raise_if_translated(exc)
            raise
        except Exception as exc:
            _raise_if_translated(exc)
            raise


_MEDIA_METHODS: dict[ContentType, tuple[str, str, str]] = {
    ContentType.PHOTO: ("send_photo", "photo", "photo"),
    ContentType.VIDEO: ("send_video", "video", "video"),
    ContentType.DOCUMENT: ("send_document", "document", "document"),
    ContentType.AUDIO: ("send_audio", "audio", "audio"),
    ContentType.ANIMATION: ("send_animation", "animation", "animation"),
    ContentType.VOICE: ("send_voice", "voice", "voice"),
    ContentType.VIDEO_NOTE: ("send_video_note", "video_note", "video_note"),
    ContentType.STICKER: ("send_sticker", "sticker", "sticker"),
}


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
    if isinstance(exc, TelegramAPIError):
        return PermanentSendError(str(exc))
    return exc


def _raise_if_translated(exc: Exception) -> None:
    translated = _translate(exc)
    if translated is not exc:
        raise translated from exc
