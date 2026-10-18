import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import DateTime, Integer, Numeric, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.database import Base


class Payment(Base):
    __tablename__ = "payments"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    description: Mapped[str] = mapped_column(String(500), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    webhook_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    payment_metadata: Mapped[dict] = mapped_column("metadata", JSONB, nullable=False, default=dict)
    webhook_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
