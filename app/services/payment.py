import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from app.models import Outbox, Payment
from app.schemas.payment import PaymentCreate


class PaymentService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_payment(
        self,
        data: PaymentCreate,
        idempotency_key: str,
    ) -> tuple[Payment, bool]:

        existing_payment = await self._get_by_idempotency_key(idempotency_key)

        if existing_payment:
            return existing_payment, False

        now = datetime.now(timezone.utc)

        payment = Payment(
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            payment_metadata=data.metadata,
            status="pending",
            idempotency_key=idempotency_key,
            webhook_url=str(data.webhook_url),
            created_at=now,
            processed_at=None,
        )

        try:
            self.session.add(payment)
            await self.session.flush()
            self.session.add(Outbox(
                event_type="payment.created",
                payload={"payment_id": str(payment.id)},
                created_at=now,
                published_at=None,
            ))
            await self.session.commit()
        except IntegrityError as exc:
            await self.session.rollback()
            # asyncpg exposes SQLSTATE on the adapter and constraint on its cause.
            original = exc.orig
            cause = getattr(original, "__cause__", None)
            if (getattr(original, "sqlstate", None) != "23505"
                    or getattr(cause, "constraint_name", None)
                    != "payments_idempotency_key_key"):
                raise

            existing_payment = await self._get_by_idempotency_key(
                idempotency_key
            )

            if existing_payment is not None:
                return existing_payment, False

            raise

        return payment, True

    async def _get_by_idempotency_key(
        self,
        idempotency_key: str,
    ) -> Payment | None:
        result = await self.session.execute(
            select(Payment).where(
                Payment.idempotency_key == idempotency_key
            )
        )

        return result.scalar_one_or_none()

    async def get_payment(
            self,
            payment_id: uuid.UUID,
    ) -> Payment | None:
        result = await self.session.execute(
            select(Payment).where(Payment.id == payment_id)
        )

        return result.scalar_one_or_none()
