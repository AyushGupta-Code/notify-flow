"""Temporal adapters over the shared notification domain service."""

from collections.abc import Callable
from typing import Any

from temporalio import activity

from notify_flow.notification_service import NotificationService
from notify_flow.types import (
    ComposeInput,
    CopyResult,
    NotificationInput,
    Plan,
    SendInput,
    SendResult,
)


class NotificationActivities(NotificationService):
    @activity.defn(name="plan_notification")
    async def plan_notification(self, inp: NotificationInput) -> Plan:
        return await self.plan(inp)

    @activity.defn(name="compose_message")
    async def compose_message(self, inp: ComposeInput) -> CopyResult:
        return await self.compose(inp)

    @activity.defn(name="send_notification")
    async def send_notification(self, inp: SendInput) -> SendResult:
        workflow_id = activity.info().workflow_id
        if workflow_id is None:  # pragma: no cover - Temporal always supplies this in an activity
            raise RuntimeError("send_notification requires a workflow ID")
        return await self.send(inp, workflow_id=workflow_id)

    def all(self) -> list[Callable[..., Any]]:
        return [self.plan_notification, self.compose_message, self.send_notification]
