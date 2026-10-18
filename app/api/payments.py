import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.database import get_db
from app.schemas.payment import PaymentCreate, PaymentCreateResponse, PaymentResponse
from app.services.payment import PaymentService
from app.core.security import verify_api_key


router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
)


@router.post(
    "",
    response_model=PaymentCreateResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_payment(
    data: PaymentCreate,
    idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=255),
    session: AsyncSession = Depends(get_db),
) -> PaymentCreateResponse:
    service = PaymentService(session)

    payment, _ = await service.create_payment(
        data=data,
        idempotency_key=idempotency_key,
    )

    return PaymentCreateResponse(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )


@router.get("/{payment_id}")
async def get_payment(
    payment_id: uuid.UUID,
    session: AsyncSession = Depends(get_db),
) -> PaymentResponse:
    service = PaymentService(session)

    payment = await service.get_payment(payment_id)

    if payment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Payment not found",
        )

    return PaymentResponse(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.payment_metadata,
        status=payment.status,
        webhook_url=payment.webhook_url,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
    )
