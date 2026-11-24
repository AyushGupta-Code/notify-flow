"""AWS Step Functions adapter. The state machine is aws/stepfunctions/notification.asl.json.

Tested against moto (see tests/integration/test_aws_adapters.py) and wired for LocalStack in
docker-compose (profile "aws"); it has not been run against real AWS.
"""

import asyncio
import dataclasses
import json
import re
from collections.abc import Awaitable, Callable
from typing import Any

from notify_flow.orchestrator.base import StartResult, workflow_id_for
from notify_flow.types import NotificationInput

_NAME_RE = re.compile(r"[^A-Za-z0-9_-]")


def execution_name(event_id: str) -> str:
    # Execution names are unique per state machine for 90 days, which gives the same
    # duplicate-start protection as a Temporal workflow ID. Max 80 chars, limited charset.
    return _NAME_RE.sub("_", workflow_id_for(event_id))[:80]


class StepFunctionsOrchestrator:
    name = "stepfunctions"

    def __init__(
        self,
        sfn_client: Any,
        state_machine_arn: str,
        task_token_lookup: Callable[[str], Awaitable[str | None]] | None = None,
    ) -> None:
        self.sfn = sfn_client
        self.arn = state_machine_arn
        # WaitForEngagement uses .waitForTaskToken; the token is stored by the
        # send step and looked up here to resume the execution.
        self.task_token_lookup = task_token_lookup

    async def start_notification(self, inp: NotificationInput) -> StartResult:
        name = execution_name(inp.event_id)
        try:
            await asyncio.to_thread(
                self.sfn.start_execution,
                stateMachineArn=self.arn,
                name=name,
                input=json.dumps(dataclasses.asdict(inp)),
            )
        except self.sfn.exceptions.ExecutionAlreadyExists:
            return StartResult(name, started=False)
        return StartResult(name, started=True)

    async def signal_engagement(self, workflow_id: str) -> None:
        if self.task_token_lookup is None:
            raise NotImplementedError("engagement signal needs a task-token store")
        token = await self.task_token_lookup(workflow_id)
        if token:
            await asyncio.to_thread(
                self.sfn.send_task_success, taskToken=token, output=json.dumps({"engaged": True})
            )
