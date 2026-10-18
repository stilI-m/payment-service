import asyncio
import logging
import uuid
from datetime import datetime, timezone

import httpx
from sqlalchemy import select

from app.db.database import async_session_maker
from app.models import Payment

MAX_ATTEMPTS = 3
logger = logging.getLogger(__name__)


class WebhookDeliveryError(RuntimeError):
    pass


async def send_webhook(payment_id: uuid.UUID) -> None:
    """Reserve each attempt durably before HTTP; never exceed three reservations."""
    async with httpx.AsyncClient(timeout=5.0) as client:
        while True:
            async with async_session_maker() as session:
                payment = (await session.execute(
                    select(Payment).where(Payment.id == payment_id).with_for_update()
                )).scalar_one()
                if payment.webhook_delivered_at is not None:
                    return
                if payment.webhook_attempts >= MAX_ATTEMPTS:
                    raise WebhookDeliveryError("Webhook attempts exhausted")
                if payment.processed_at is None:
                    raise ValueError("Payment has not been processed")
                payment.webhook_attempts += 1
                attempt = payment.webhook_attempts
                url = payment.webhook_url
                payload = {
                    "payment_id": str(payment.id),
                    "status": payment.status,
                    "processed_at": payment.processed_at.isoformat(),
                }
                # A crash after this commit consumes the reserved attempt.
                await session.commit()

            if attempt > 1:
                await asyncio.sleep(2 ** (attempt - 2))
            try:
                response = await client.post(url, json=payload)
                response.raise_for_status()
            except httpx.HTTPError as exc:
                logger.warning("Webhook %s attempt %s/%s failed: %s",
                               payment_id, attempt, MAX_ATTEMPTS, type(exc).__name__)
                if attempt == MAX_ATTEMPTS:
                    raise WebhookDeliveryError("Webhook failed after 3 attempts") from exc
                continue

            async with async_session_maker() as session:
                payment = (await session.execute(
                    select(Payment).where(Payment.id == payment_id).with_for_update()
                )).scalar_one()
                payment.webhook_delivered_at = datetime.now(timezone.utc)
                await session.commit()
            return
