"""Set TELEGRAM_BOT_TOKEN; replace recipient IDs before running."""

import asyncio
import os

from telegram_broadcast import BroadcastClient, BroadcastMessage


async def main() -> None:
    async with BroadcastClient(
        os.environ["TELEGRAM_BOT_TOKEN"], database_path="data/broadcasts.sqlite3"
    ) as broadcasts:
        job_id = await broadcasts.create(
            BroadcastMessage("<b>Hello!</b>"),
            recipient_ids=[123456789],
            notification_chat_ids=[123456789],
            title="Example broadcast",
        )
        result = await broadcasts.run(job_id)
        print(job_id, result.status, result.success, result.failed, result.unknown)
        print(await broadcasts.get_deliveries(job_id))


if __name__ == "__main__":
    asyncio.run(main())
