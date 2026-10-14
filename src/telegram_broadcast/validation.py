"""Read-only Telegram preflight before persisting a new broadcast."""

from collections.abc import Sequence

from aiogram import Bot
from aiogram.enums import ChatType
from aiogram.exceptions import (
    TelegramAPIError,
    TelegramRetryAfter,
    TelegramUnauthorizedError,
)
from aiogram.types import (
    ChatFullInfo,
    ChatMemberAdministrator,
    ChatMemberBanned,
    ChatMemberLeft,
    ChatMemberMember,
    ChatMemberOwner,
    ChatMemberRestricted,
)
from aiogram.types.chat_member_union import ChatMemberUnion

from .errors import PreflightError
from .sanitization import ErrorSanitizer
from .types import NotificationRecipient


async def validate_configuration(
    bot: Bot, notifications: Sequence[NotificationRecipient]
) -> None:
    sanitize = ErrorSanitizer(bot.token)
    try:
        identity = await bot.get_me()
    except TelegramAPIError as exc:
        raise PreflightError((f"Токен бота: {sanitize(exc)}",)) from None

    issues: list[str] = []
    # Multiple topics in a chat share the same membership and permissions.
    chat_ids = dict.fromkeys(recipient.chat_id for recipient in notifications)
    topic_chats = {item.chat_id for item in notifications if item.thread_id is not None}
    for chat_id in chat_ids:
        try:
            chat = await bot.get_chat(chat_id)
            reason = None
            if chat.type != ChatType.PRIVATE:
                member = await bot.get_chat_member(chat_id, identity.id)
                reason = _permission_error(chat, member)
            if (
                reason is None
                and chat_id in topic_chats
                and chat.type in (ChatType.GROUP, ChatType.SUPERGROUP)
                and not chat.is_forum
            ):
                reason = "указан thread_id, но в группе не включены темы"
            if reason:
                issues.append(f"Чат администратора {chat_id}: {reason}")
        except TelegramAPIError as exc:
            issues.append(f"Чат администратора {chat_id}: {sanitize(exc)}")
            if isinstance(exc, (TelegramRetryAfter, TelegramUnauthorizedError)):
                break
    if issues:
        raise PreflightError(tuple(issues))


def _permission_error(chat: ChatFullInfo, member: ChatMemberUnion) -> str | None:
    if isinstance(member, (ChatMemberBanned, ChatMemberLeft)):
        return "бот не состоит в чате или заблокирован"
    if chat.type == ChatType.CHANNEL:
        if isinstance(member, ChatMemberOwner):
            return None
        if isinstance(member, ChatMemberAdministrator) and member.can_post_messages:
            return None
        return "боту не разрешена публикация сообщений в канале"
    if isinstance(member, ChatMemberRestricted):
        if not member.is_member or not member.can_send_messages:
            return "боту запрещено отправлять сообщения"
    if isinstance(member, ChatMemberMember) and chat.permissions is not None:
        if chat.permissions.can_send_messages is False:
            return "настройки группы запрещают боту отправлять сообщения"
    return None
