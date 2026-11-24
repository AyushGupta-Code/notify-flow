"""durable cross-campaign frequency-cap reservations

Revision ID: 0002
Revises: 0001
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "notification_reservations",
        sa.Column("event_id", sa.String(length=128), nullable=False),
        sa.Column("member_id", sa.String(length=64), nullable=False),
        sa.Column(
            "reserved_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("event_id"),
    )
    op.create_index(
        op.f("ix_notification_reservations_member_id"),
        "notification_reservations",
        ["member_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_notification_reservations_reserved_at"),
        "notification_reservations",
        ["reserved_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_notification_reservations_reserved_at"),
        table_name="notification_reservations",
    )
    op.drop_index(
        op.f("ix_notification_reservations_member_id"),
        table_name="notification_reservations",
    )
    op.drop_table("notification_reservations")
