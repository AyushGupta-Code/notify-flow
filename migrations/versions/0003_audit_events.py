"""append-only API security audit events

Revision ID: 0003
Revises: 0002
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "audit_events",
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), nullable=False),
        sa.Column("actor_hash", sa.String(length=64), nullable=False),
        sa.Column("method", sa.String(length=16), nullable=False),
        sa.Column("route", sa.String(length=200), nullable=False),
        sa.Column("status", sa.Integer(), nullable=False),
        sa.Column("correlation_id", sa.String(length=64), nullable=True),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_audit_events_route"), "audit_events", ["route"], unique=False)
    op.create_index(
        op.f("ix_audit_events_correlation_id"),
        "audit_events",
        ["correlation_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_audit_events_occurred_at"), "audit_events", ["occurred_at"], unique=False
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_audit_events_occurred_at"), table_name="audit_events")
    op.drop_index(op.f("ix_audit_events_correlation_id"), table_name="audit_events")
    op.drop_index(op.f("ix_audit_events_route"), table_name="audit_events")
    op.drop_table("audit_events")
