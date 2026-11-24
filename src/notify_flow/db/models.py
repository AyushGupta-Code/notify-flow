"""SQLAlchemy models. Alembic migration 0001 mirrors this file."""

from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

JsonType = JSON().with_variant(JSONB(), "postgresql")


class Base(DeclarativeBase):
    pass


class Member(Base):
    """A synthetic program member. Identifiers here never leave the service boundary
    except to the channel provider at send time."""

    __tablename__ = "members"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    first_name: Mapped[str] = mapped_column(String(80))
    email: Mapped[str | None] = mapped_column(String(200))
    phone: Mapped[str | None] = mapped_column(String(32))
    timezone: Mapped[str] = mapped_column(String(64), default="America/New_York")
    program_week: Mapped[int] = mapped_column(Integer, default=1)
    sessions_last_7d: Mapped[int] = mapped_column(Integer, default=0)
    days_since_last_session: Mapped[int] = mapped_column(Integer, default=0)
    preferred_channel: Mapped[str | None] = mapped_column(String(16))
    sms_opt_in: Mapped[bool] = mapped_column(Boolean, default=True)
    push_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    email_opt_in: Mapped[bool] = mapped_column(Boolean, default=True)
    open_rate_sms: Mapped[float] = mapped_column(default=0.3)
    open_rate_push: Mapped[float] = mapped_column(default=0.3)
    open_rate_email: Mapped[float] = mapped_column(default=0.3)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class InboundEvent(Base):
    """Every accepted event, keyed by the producer's event_id (the dedup key)."""

    __tablename__ = "inbound_events"

    event_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    member_id: Mapped[str] = mapped_column(String(64), index=True)
    event_type: Mapped[str] = mapped_column(String(64))
    schema_version: Mapped[int] = mapped_column(Integer, default=1)
    payload: Mapped[dict[str, Any]] = mapped_column(JsonType)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    correlation_id: Mapped[str | None] = mapped_column(String(64))


class Outbox(Base):
    """Transactional outbox: written in the same transaction as InboundEvent and relayed
    to the queue afterwards, so an accepted event is never lost between DB and queue."""

    __tablename__ = "outbox"

    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    event_id: Mapped[str] = mapped_column(ForeignKey("inbound_events.event_id"), unique=True)
    topic: Mapped[str] = mapped_column(String(128))
    key: Mapped[str] = mapped_column(String(128))
    payload: Mapped[dict[str, Any]] = mapped_column(JsonType)
    headers: Mapped[dict[str, Any]] = mapped_column(JsonType, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)


class ProcessedEvent(Base):
    """Consumer-side record of events already handed to the orchestrator."""

    __tablename__ = "processed_events"

    event_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    workflow_id: Mapped[str] = mapped_column(String(200))
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Notification(Base):
    """One row per (event, step). idempotency_key is unique, which is what makes a send
    happen at most once no matter how many times the activity runs."""

    __tablename__ = "notifications"
    __table_args__ = (UniqueConstraint("idempotency_key", name="uq_notifications_idem"),)

    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String(200))
    member_id: Mapped[str] = mapped_column(String(64), index=True)
    event_id: Mapped[str] = mapped_column(String(128), index=True)
    workflow_id: Mapped[str] = mapped_column(String(200))
    campaign: Mapped[str] = mapped_column(String(64))
    step: Mapped[int] = mapped_column(Integer)
    channel: Mapped[str] = mapped_column(String(16))
    status: Mapped[str] = mapped_column(String(16), default="pending")
    body_template: Mapped[str] = mapped_column(Text)
    copy_source: Mapped[str] = mapped_column(String(16))
    copy_reason: Mapped[str | None] = mapped_column(String(64))
    prompt_version: Mapped[str | None] = mapped_column(String(64))
    llm_model: Mapped[str | None] = mapped_column(String(128))
    template_version: Mapped[str | None] = mapped_column(String(64))
    channel_model_version: Mapped[str | None] = mapped_column(String(64))
    decision_source: Mapped[str] = mapped_column(String(16))
    correlation_id: Mapped[str | None] = mapped_column(String(64))
    provider_message_id: Mapped[str | None] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    engaged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ProviderDelivery(Base):
    """Stands in for the SMS/push/email vendor's own store. Real vendors (Twilio, SES,
    FCM) are called with our idempotency key; this table plays that role locally so
    tests can count what a member would actually have received."""

    __tablename__ = "provider_deliveries"

    idempotency_key: Mapped[str] = mapped_column(String(200), primary_key=True)
    provider_message_id: Mapped[str] = mapped_column(String(128))
    channel: Mapped[str] = mapped_column(String(16))
    member_id: Mapped[str] = mapped_column(String(64), index=True)
    delivered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
