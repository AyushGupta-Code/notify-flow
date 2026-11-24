"""Real Step Functions + Lambda executions against LocalStack.

This module is skipped in the normal test job and enabled only by the dedicated
LocalStack CI job. No moto or mocked AWS clients are used here.
"""

import asyncio
import dataclasses
import json
import os
import time
import uuid

import boto3
import pytest
from sqlalchemy import select

from notify_flow.db.models import Notification
from notify_flow.orchestrator.stepfunctions import StepFunctionsOrchestrator, execution_name
from notify_flow.types import NotificationInput

from .helpers import add_member, deliveries

ENDPOINT = os.getenv("NF_LOCALSTACK_ENDPOINT")
STATE_MACHINE_ARN = os.getenv("NF_SFN_STATE_MACHINE_ARN")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not ENDPOINT or not STATE_MACHINE_ARN,
        reason="dedicated LocalStack state-machine job only",
    ),
]


@pytest.fixture
def sfn():
    return boto3.client("stepfunctions", endpoint_url=ENDPOINT, region_name="us-east-1")


def notification(event_id: str | None = None, **extra):
    inp = NotificationInput(
        event_id or f"evt-localstack-{uuid.uuid4().hex[:12]}",
        "m-001",
        "session_missed",
        "reengage",
        {},
        "2026-11-20T15:00:00+00:00",
        "cid-localstack",
        0,
    )
    return {**dataclasses.asdict(inp), "force_engaged": True, **extra}


def wait_for_execution(sfn, execution_arn: str) -> dict:
    for _ in range(180):
        description = sfn.describe_execution(executionArn=execution_arn)
        if description["status"] != "RUNNING":
            assert description["status"] == "SUCCEEDED", description
            return json.loads(description["output"])
        time.sleep(1)
    raise AssertionError("state-machine execution did not finish within 180 seconds")


def start(sfn, state: dict) -> tuple[str, dict]:
    response = sfn.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        name=execution_name(state["event_id"]),
        input=json.dumps(state),
    )
    return response["executionArn"], wait_for_execution(sfn, response["executionArn"])


async def test_happy_path_sends_exactly_one_message(sfn, sm):
    await add_member(sm)
    state = notification()
    _, output = await asyncio.to_thread(start, sfn, state)
    assert len(output["sends"]) == 1 and output["sends"][0]["status"] == "sent"
    assert await deliveries(sm, "m-001") == 1


async def test_duplicate_start_gives_one_execution_and_one_message(sfn, sm):
    await add_member(sm)
    state = notification("evt-localstack-duplicate")
    state.update(campaign="pain_checkin", event_type="pain_score_reported")
    orchestrator = StepFunctionsOrchestrator(sfn, STATE_MACHINE_ARN)
    inp = NotificationInput(
        **{field.name: state[field.name] for field in dataclasses.fields(NotificationInput)}
    )
    first = await orchestrator.start_notification(inp)
    second = await orchestrator.start_notification(inp)
    assert first.started is True and second.started is False
    execution_arn = next(
        item["executionArn"]
        for item in sfn.list_executions(stateMachineArn=STATE_MACHINE_ARN)["executions"]
        if item["name"] == execution_name(inp.event_id)
    )
    await asyncio.to_thread(wait_for_execution, sfn, execution_arn)
    matching = [
        item
        for item in sfn.list_executions(stateMachineArn=STATE_MACHINE_ARN)["executions"]
        if item["name"] == execution_name(inp.event_id)
    ]
    assert len(matching) == 1
    assert await deliveries(sm, "m-001") == 1


async def test_forced_lambda_failure_retries_then_catches(sfn, sm):
    await add_member(sm)
    execution_arn, output = await asyncio.to_thread(
        start, sfn, notification(force_failure="quiet_hours")
    )
    assert output["status"] == "caught"
    history = sfn.get_execution_history(executionArn=execution_arn, maxResults=1000)["events"]
    scheduled = [
        event for event in history if event["type"] in {"TaskScheduled", "LambdaFunctionScheduled"}
    ]
    failed = [event for event in history if event["type"] in {"TaskFailed", "LambdaFunctionFailed"}]
    assert len(scheduled) >= 4
    assert len(failed) >= 3
    assert await deliveries(sm, "m-001") == 0


async def test_llm_down_falls_back_and_sends(sfn, sm):
    await add_member(sm)
    state = notification(llm_mode="down")
    _, output = await asyncio.to_thread(start, sfn, state)
    assert len(output["sends"]) == 1
    async with sm() as session:
        row = (
            await session.execute(
                select(Notification).where(Notification.event_id == state["event_id"])
            )
        ).scalar_one()
    assert row.copy_source == "template" and row.copy_reason == "llm_error"
    assert await deliveries(sm, "m-001") == 1
