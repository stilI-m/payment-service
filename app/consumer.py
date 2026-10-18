import asyncio
import logging
import random
import uuid
from datetime import datetime, timezone

from faststream import FastStream
from faststream.exceptions import RejectMessage
from faststream.middlewares import AckPolicy
from sqlalchemy import select
from sqlalchemy.exc import OperationalError, InterfaceError

from app.broker.rabbit import PAYMENTS_QUEUE, broker, declare_topology
from app.db.database import async_session_maker
from app.models import Payment
from app.services.webhook import send_webhook

app = FastStream(broker)
logger = logging.getLogger(__name__)


@app.on_startup
async def setup_topology() -> None:
    await broker.connect()
    await declare_topology()


async def process_payment_once(payment_id: uuid.UUID) -> None:
    async with async_session_maker() as session:
        payment = (await session.execute(
            select(Payment).where(Payment.id == payment_id).with_for_update()
        )).scalar_one_or_none()
        if payment is None:
            raise ValueError(f"Payment {payment_id} not found")
        if payment.status == "pending":
            await asyncio.sleep(random.uniform(2, 5))
            payment.status = "succeeded" if random.random() < 0.9 else "failed"
            payment.processed_at = datetime.now(timezone.utc)
        await session.commit()
    # A completed payment may still have an undelivered webhook.
    await send_webhook(payment_id)


@broker.subscriber(PAYMENTS_QUEUE, ack_policy=AckPolicy.REJECT_ON_ERROR)
async def process_payment(message: dict) -> None:
    payment_id = uuid.UUID(message["payment_id"])
    for attempt in range(1, 4):
        try:
            await process_payment_once(payment_id)
            return  # FastStream ACK only after completion.
        except (OperationalError, InterfaceError, OSError, asyncio.TimeoutError):
            logger.exception("Payment %s database attempt %s/3 failed", payment_id, attempt)
            if attempt == 3:
                raise RejectMessage(requeue=False)
            await asyncio.sleep(2 ** (attempt - 1))
    # Other errors (including exhausted webhooks) are rejected by AckPolicy.
