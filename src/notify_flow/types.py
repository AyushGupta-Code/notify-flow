"""Plain dataclasses passed between the consumer, the orchestrator and activities.
Kept free of heavy imports so the Temporal workflow sandbox can load them."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class NotificationInput:
    event_id: str
    member_id: str
    event_type: str
    campaign: str
    payload: dict[str, Any]
    occurred_at: str  # ISO-8601
    correlation_id: str | None = None
    engagement_wait_s: int = 48 * 3600


@dataclass
class Plan:
    skip: bool
    reason: str
    channel: str = ""
    escalation_channel: str | None = None
    decision_source: str = "rules"
    channel_model_version: str | None = None
    timezone: str = "UTC"
    quiet_hours: tuple[int, int] | None = None  # (start, end) local hours; None = disabled
    context: dict[str, Any] = field(default_factory=dict)  # non-identifying features only


@dataclass(frozen=True)
class CopyResult:
    body_template: str
    source: str  # "llm" | "template"
    reason: str  # "ok" | "policy" | "llm_error" | "circuit_open" | "guardrail"
    prompt_version: str | None
    llm_model: str | None
    template_version: str
    registry_version: str


@dataclass
class ComposeInput:
    member_id: str
    campaign: str
    channel: str
    context: dict[str, Any]
    correlation_id: str | None = None


@dataclass
class SendInput:
    notification: NotificationInput
    plan: Plan
    copy: CopyResult
    step: int
    channel: str


@dataclass
class SendResult:
    notification_id: int
    status: str  # "sent" | "duplicate"
    provider_message_id: str | None
    channel: str = ""
