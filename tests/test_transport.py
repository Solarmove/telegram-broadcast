from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiogram.enums import ContentType
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramNotFound,
    TelegramRetryAfter,
    TelegramServerError,
    TelegramUnauthorizedError,
)
from aiogram.methods import SendMessage
from aiogram.types import Chat, FSInputFile, Message
from conftest import TOKEN

from telegram_broadcast import (
    BroadcastMedia,
    BroadcastMessage,
    DeliveryUncertain,
    PermanentSendError,
    RateLimited,
    RecipientUnavailable,
)
from telegram_broadcast.transport import AiogramTransport


@pytest.mark.parametrize(
    "exception, expected",
    [
        (TelegramForbiddenError, RecipientUnavailable),
        (TelegramNotFound, RecipientUnavailable),
        (TelegramNetworkError, DeliveryUncertain),
        (TelegramServerError, DeliveryUncertain),
        (TelegramUnauthorizedError, PermanentSendError),
        (TelegramBadRequest, PermanentSendError),
    ],
)
async def test_error_translation(exception, expected):
    adapter = AiogramTransport(TOKEN)
    method = SendMessage(chat_id=1, text="hello")
    adapter._bot.send_message = AsyncMock(
        side_effect=exception(method=method, message="error")
    )
    with pytest.raises(expected):
        await adapter.send(1, BroadcastMessage("hello"), None)
    await adapter.close()


async def test_rate_limit_translation():
    adapter = AiogramTransport(TOKEN)
    method = SendMessage(chat_id=1, text="hello")
    adapter._bot.send_message = AsyncMock(
        side_effect=TelegramRetryAfter(method=method, message="limited", retry_after=42)
    )
    with pytest.raises(RateLimited) as error:
        await adapter.send(1, BroadcastMessage("hello"), None)
    assert error.value.retry_after == 42
    await adapter.close()


async def test_chat_not_found_is_recipient_specific():
    adapter = AiogramTransport(TOKEN)
    adapter._bot.send_message = AsyncMock(
        side_effect=TelegramBadRequest(
            method=SendMessage(chat_id=1, text="hello"),
            message="Bad Request: chat not found",
        )
    )
    with pytest.raises(RecipientUnavailable):
        await adapter.send(1, BroadcastMessage("hello"), None)
    await adapter.close()


async def test_photo_url_is_sent_and_returned_file_id_is_cached():
    adapter = AiogramTransport(TOKEN)
    photo = SimpleNamespace(file_id="reusable-file")
    adapter._bot.send_photo = AsyncMock(
        return_value=SimpleNamespace(message_id=9, photo=[photo])
    )
    result = await adapter.send(
        1,
        BroadcastMessage(
            "Caption",
            media=BroadcastMedia(
                ContentType.PHOTO, url="https://example.org/photo.jpg"
            ),
        ),
        None,
    )
    adapter._bot.send_photo.assert_awaited_once_with(
        1,
        photo="https://example.org/photo.jpg",
        caption="Caption",
        parse_mode="HTML",
    )
    assert result.message_id == 9
    assert result.media_file_id == "reusable-file"
    await adapter.close()


async def test_local_document_is_wrapped_as_input_file(tmp_path):
    adapter = AiogramTransport(TOKEN)
    adapter._bot.send_document = AsyncMock(
        return_value=SimpleNamespace(
            message_id=10, document=SimpleNamespace(file_id="document-id")
        )
    )
    result = await adapter.send(
        1,
        BroadcastMessage(
            "Document",
            media=BroadcastMedia(
                ContentType.DOCUMENT, path_to_file=str(tmp_path / "doc.pdf")
            ),
        ),
        None,
    )
    call = adapter._bot.send_document.await_args
    assert call is not None
    sent_file = call.kwargs["document"]
    assert isinstance(sent_file, FSInputFile)
    assert result.media_file_id == "document-id"
    await adapter.close()


async def test_report_is_sent_to_requested_topic():
    adapter = AiogramTransport(TOKEN)
    adapter._bot.send_message = AsyncMock(return_value=SimpleNamespace(message_id=11))
    assert await adapter.report(1, "Progress", None, thread_id=42) == 11
    adapter._bot.send_message.assert_awaited_once_with(
        1,
        "Progress",
        parse_mode=None,
        message_thread_id=42,
    )
    await adapter.close()


async def test_unchanged_report_is_success():
    adapter = AiogramTransport(TOKEN)
    adapter._bot.edit_message_text = AsyncMock(
        side_effect=TelegramBadRequest(
            method=SendMessage(chat_id=1, text="hello"),
            message="Bad Request: message is not modified",
        )
    )
    assert await adapter.report(1, "same text", 12) == 12
    await adapter.close()


async def test_deleted_report_is_replaced():
    adapter = AiogramTransport(TOKEN)
    adapter._bot.edit_message_text = AsyncMock(
        side_effect=TelegramBadRequest(
            method=SendMessage(chat_id=1, text="hello"),
            message="Bad Request: message to edit not found",
        )
    )
    adapter._bot.send_message = AsyncMock(
        return_value=Message(
            message_id=13,
            date=datetime.now(UTC),
            chat=Chat(id=1, type="private"),
            text="hello",
        )
    )
    assert await adapter.report(1, "hello", 12) == 13
    adapter._bot.send_message.assert_awaited_once()
    await adapter.close()
