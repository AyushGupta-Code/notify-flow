"""Versioned member-event schema shared by producers, the API and the consumer."""

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

EventType = Literal["enrolled", "session_missed", "session_completed", "pain_score_reported"]


class MemberEvent(BaseModel):
    event_id: str = Field(min_length=8, max_length=128, description="Producer-assigned dedup key")
    schema_version: Literal[1] = 1
    member_id: str = Field(min_length=1, max_length=64)
    event_type: EventType
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    payload: dict[str, Any] = Field(default_factory=dict)


def campaign_for(event: MemberEvent) -> str | None:
    """Map an event to the campaign it triggers, or None when no message is warranted."""
    if event.event_type == "enrolled":
        return "welcome"
    if event.event_type == "session_missed":
        return "reengage"
    if event.event_type == "session_completed":
        count = int(event.payload.get("session_count", 0))
        return "milestone" if count > 0 and count % 5 == 0 else None
    if event.event_type == "pain_score_reported":
        return "pain_checkin" if int(event.payload.get("score", 0)) >= 7 else None
    return None
