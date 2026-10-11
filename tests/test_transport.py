from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
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
from aiogram.types import Chat, Message
from conftest import TOKEN

from telegram_broadcast import (
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
