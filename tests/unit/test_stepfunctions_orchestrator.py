import asyncio
from types import SimpleNamespace

from notify_flow.orchestrator.stepfunctions import StepFunctionsOrchestrator, execution_arn
from notify_flow.types import NotificationInput


class ExecutionDoesNotExist(Exception):
    pass


class ExecutionAlreadyExists(Exception):
    pass


class FakeStepFunctions:
    exceptions = SimpleNamespace(
        ExecutionDoesNotExist=ExecutionDoesNotExist,
        ExecutionAlreadyExists=ExecutionAlreadyExists,
    )

    def __init__(self):
        self.executions = set()
        self.starts = 0

    def describe_execution(self, *, executionArn):
        if executionArn not in self.executions:
            raise ExecutionDoesNotExist
        return {"executionArn": executionArn, "status": "RUNNING"}

    def start_execution(self, *, stateMachineArn, name, input):
        arn = execution_arn(stateMachineArn, name)
        if arn in self.executions:
            raise ExecutionAlreadyExists
        self.executions.add(arn)
        self.starts += 1
        return {"executionArn": arn}


def test_duplicate_start_is_reported_without_starting_again():
    client = FakeStepFunctions()
    orchestrator = StepFunctionsOrchestrator(
        client, "arn:aws:states:us-east-1:123456789012:stateMachine:notify-flow"
    )
    notification = NotificationInput(
        "evt-1", "member-1", "session_missed", "reengage", {}, "2026-11-24T00:00:00Z", "cid"
    )

    first = asyncio.run(orchestrator.start_notification(notification))
    second = asyncio.run(orchestrator.start_notification(notification))

    assert first.started is True
    assert second.started is False
    assert client.starts == 1
