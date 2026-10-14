from unittest.mock import AsyncMock

import pytest
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramRetryAfter,
    TelegramUnauthorizedError,
)
from aiogram.methods import GetChat, GetMe
from aiogram.types import (
    ChatFullInfo,
    ChatMemberAdministrator,
    ChatMemberLeft,
    ChatMemberMember,
    ChatMemberRestricted,
    ChatPermissions,
    User,
)
from conftest import TOKEN
from sqlalchemy import func, select

from telegram_broadcast import (
    BroadcastClient,
    BroadcastMessage,
    NotificationRecipient,
    PreflightError,
)
from telegram_broadcast.transport import AiogramTransport


@pytest.fixture
async def adapter():
    transport = AiogramTransport(TOKEN)
    bot = transport._bot
    bot.get_me = AsyncMock(return_value=User(id=123456, is_bot=True, first_name="test"))
    bot.get_chat = AsyncMock(
        return_value=ChatFullInfo.model_construct(
            id=-10,
            type="supergroup",
            is_forum=True,
            permissions=ChatPermissions(can_send_messages=True),
        )
    )
    bot.get_chat_member = AsyncMock(return_value=ChatMemberMember.model_construct())
    try:
        yield transport
    finally:
        await transport.close()


async def test_invalid_remote_token_blocks_create_without_writing_database(
    adapter, tmp_path
):
    adapter._bot.get_me.side_effect = TelegramUnauthorizedError(
        method=GetMe(),
        message=f"Unauthorized {TOKEN}",
    )
    path = tmp_path / "unused.sqlite3"
    client = BroadcastClient(TOKEN, database_path=path, transport=adapter)
    try:
        with pytest.raises(PreflightError) as failure:
            await client.create(BroadcastMessage("test"), [1])
        assert TOKEN not in str(failure.value)
        assert "Unauthorized" in str(failure.value)
        assert not path.exists()
        adapter._bot.get_chat.assert_not_awaited()
    finally:
        await client.close()


async def test_all_missing_admin_chats_are_reported(adapter):
    async def missing(chat_id):
        raise TelegramBadRequest(
            method=GetChat(chat_id=chat_id), message="chat not found"
        )

    adapter._bot.get_chat.side_effect = missing
    with pytest.raises(PreflightError) as failure:
        await adapter.validate([NotificationRecipient(-10), NotificationRecipient(-20)])
    assert len(failure.value.issues) == 2
    assert "-10" in str(failure.value) and "-20" in str(failure.value)
    assert "chat not found" in str(failure.value)


async def test_failed_admin_preflight_does_not_persist_any_job(harness, adapter):
    client, _, engine = harness
    client._transport = adapter
    adapter._bot.get_chat.side_effect = [
        adapter._bot.get_chat.return_value,
        TelegramBadRequest(method=GetChat(chat_id=-20), message="chat not found"),
    ]
    with pytest.raises(PreflightError):
        await client.create(
            BroadcastMessage("test"), [1], notification_chats=[-10, -20]
        )
    async with engine.connect() as connection:
        for table in (
            client._tables.broadcasts,
            client._tables.deliveries,
            client._tables.notifications,
        ):
            assert await connection.scalar(select(func.count()).select_from(table)) == 0


async def test_token_and_chats_rechecked_on_each_create_but_topics_share_check(
    adapter, tmp_path
):
    async with BroadcastClient(
        TOKEN, database_path=tmp_path / "jobs.sqlite3", transport=adapter
    ) as client:
        for _ in range(2):
            await client.create(
                BroadcastMessage("test"),
                [1],
                notification_chats=[
                    NotificationRecipient(-10, 2),
                    NotificationRecipient(-10, 3),
                ],
            )
    assert adapter._bot.get_me.await_count == 2
    assert adapter._bot.get_chat.await_count == 2
    assert adapter._bot.get_chat_member.await_count == 2


@pytest.mark.parametrize(
    "chat_type,member,allowed",
    [
        ("supergroup", ChatMemberLeft.model_construct(), False),
        (
            "supergroup",
            ChatMemberRestricted.model_construct(
                is_member=True, can_send_messages=False
            ),
            False,
        ),
        (
            "supergroup",
            ChatMemberRestricted.model_construct(
                is_member=True, can_send_messages=True
            ),
            True,
        ),
        ("channel", ChatMemberMember.model_construct(), False),
        (
            "channel",
            ChatMemberAdministrator.model_construct(can_post_messages=False),
            False,
        ),
        (
            "channel",
            ChatMemberAdministrator.model_construct(can_post_messages=True),
            True,
        ),
    ],
)
async def test_membership_and_posting_permissions(adapter, chat_type, member, allowed):
    adapter._bot.get_chat.return_value = ChatFullInfo.model_construct(
        id=-10, type=chat_type
    )
    adapter._bot.get_chat_member.return_value = member
    if allowed:
        await adapter.validate([NotificationRecipient(-10)])
    else:
        with pytest.raises(PreflightError, match="-10"):
            await adapter.validate([NotificationRecipient(-10)])


async def test_group_default_permissions_are_checked(adapter):
    adapter._bot.get_chat.return_value = ChatFullInfo.model_construct(
        id=-10,
        type="group",
        permissions=ChatPermissions(can_send_messages=False),
    )
    with pytest.raises(PreflightError, match="запрещают"):
        await adapter.validate([NotificationRecipient(-10)])


async def test_private_chat_does_not_require_membership_lookup(adapter):
    adapter._bot.get_chat.return_value = ChatFullInfo.model_construct(
        id=10, type="private"
    )
    await adapter.validate([NotificationRecipient(10)])
    adapter._bot.get_chat_member.assert_not_awaited()


async def test_empty_notification_list_still_validates_token(adapter):
    await adapter.validate([])
    adapter._bot.get_me.assert_awaited_once()
    adapter._bot.get_chat.assert_not_awaited()


async def test_thread_in_non_forum_group_is_rejected(adapter):
    adapter._bot.get_chat.return_value = ChatFullInfo.model_construct(
        id=-10,
        type="supergroup",
        is_forum=False,
    )
    with pytest.raises(PreflightError, match="thread_id"):
        await adapter.validate([NotificationRecipient(-10, thread_id=42)])


async def test_rate_limit_blocks_create_and_stops_further_validation_requests(adapter):
    adapter._bot.get_chat.side_effect = TelegramRetryAfter(
        method=GetChat(chat_id=-10),
        message="limited",
        retry_after=60,
    )
    with pytest.raises(PreflightError, match="60"):
        await adapter.validate([NotificationRecipient(-10), NotificationRecipient(-20)])
    assert adapter._bot.get_chat.await_count == 1
