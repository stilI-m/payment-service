import asyncio
import logging

from datetime import datetime, timezone

from sqlalchemy import select

from app.broker.rabbit import PAYMENTS_QUEUE, broker
from app.db.database import async_session_maker
from app.models import Outbox


async def publish_outbox_events() -> None:
    async with async_session_maker() as session:
        result = await session.execute(
            select(Outbox)
            .where(Outbox.published_at.is_(None))
            .order_by(Outbox.created_at)
            .limit(100)
            .with_for_update(skip_locked=True)
        )

        events = result.scalars().all()

        for event in events:
            await broker.publish(
                event.payload,
                queue=PAYMENTS_QUEUE,
                message_id=str(event.id),
                persist=True,
                timeout=10,
            )

            event.published_at = datetime.now(timezone.utc)

        await session.commit()


async def outbox_publisher_loop() -> None:
    while True:
        try:
            await publish_outbox_events()
        except Exception:
            logging.getLogger(__name__).exception("Outbox publication failed")

        await asyncio.sleep(2)
