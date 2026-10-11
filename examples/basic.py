"""Set DATABASE_URL and TELEGRAM_BOT_TOKEN; replace recipient IDs before running."""

import asyncio
import os

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase

from telegram_broadcast import BroadcastClient, BroadcastMessage


class Base(DeclarativeBase):
    pass


async def main() -> None:
    engine = create_async_engine(os.environ["DATABASE_URL"])
    try:
        async with BroadcastClient(
            os.environ["TELEGRAM_BOT_TOKEN"], base=Base, engine=engine
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
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
