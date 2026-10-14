"""Set TELEGRAM_BOT_TOKEN, RECIPIENT_CHAT_ID, ADMIN_CHAT_ID; optionally ADMIN_THREAD_ID."""

import asyncio
import logging
import os

from telegram_broadcast import (
    BroadcastClient,
    BroadcastMessage,
    NotificationRecipient,
    PreflightError,
    RunPolicy,
)

logger = logging.getLogger(__name__)


async def main() -> None:
    thread_id = os.environ.get("ADMIN_THREAD_ID")
    async with BroadcastClient(
        os.environ["TELEGRAM_BOT_TOKEN"],
        database_path="data/broadcasts.sqlite3",
        policy=RunPolicy(report_interval=5),
    ) as broadcasts:
        broadcast_id = await broadcasts.create(
            BroadcastMessage("test"),
            recipient_ids=[int(os.environ["RECIPIENT_CHAT_ID"])],
            notification_chats=[
                NotificationRecipient(
                    chat_id=int(os.environ["ADMIN_CHAT_ID"]),
                    thread_id=int(thread_id) if thread_id else None,
                )
            ],
        )
        logger.info("Broadcast ID: %s", broadcast_id)
        result = await broadcasts.run(broadcast_id)
        for notification in result.notifications:
            logger.info(
                "Report: chat_id=%s thread_id=%s message_id=%s error=%s",
                notification.chat_id,
                notification.thread_id,
                notification.message_id,
                notification.error,
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(main())
    except PreflightError as error:
        logger.error("%s", error)
        raise SystemExit(1) from None
