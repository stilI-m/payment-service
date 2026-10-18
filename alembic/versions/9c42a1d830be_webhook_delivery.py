"""Track webhook delivery across message redeliveries."""
from alembic import op
import sqlalchemy as sa

revision = "9c42a1d830be"
down_revision = "70670276ec1f"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("payments", sa.Column("webhook_attempts", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("payments", sa.Column("webhook_delivered_at", sa.DateTime(timezone=True), nullable=True))


def downgrade():
    op.drop_column("payments", "webhook_delivered_at")
    op.drop_column("payments", "webhook_attempts")
