import uuid
from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field, HttpUrl


class PaymentCreate(BaseModel):
    amount: Decimal = Field(gt=0, max_digits=12, decimal_places=2)
    currency: Literal["RUB", "USD", "EUR"]
    description: str = Field(min_length=1, max_length=500)
    metadata: dict = Field(default_factory=dict)
    webhook_url: HttpUrl


class PaymentCreateResponse(BaseModel):
    payment_id: uuid.UUID
    status: str
    created_at: datetime


class PaymentResponse(BaseModel):
    payment_id: uuid.UUID
    amount: Decimal
    currency: str
    description: str
    metadata: dict
    status: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
