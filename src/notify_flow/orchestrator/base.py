from dataclasses import dataclass
from typing import Protocol

from notify_flow.types import NotificationInput


@dataclass(frozen=True)
class StartResult:
    workflow_id: str
    started: bool  # False when a workflow for this event already exists


def workflow_id_for(event_id: str) -> str:
    """Deterministic ID: the orchestrator itself rejects a second start for the same event."""
    return f"notify-{event_id}"


class Orchestrator(Protocol):
    name: str

    async def start_notification(self, inp: NotificationInput) -> StartResult: ...

    async def signal_engagement(self, workflow_id: str) -> None: ...
