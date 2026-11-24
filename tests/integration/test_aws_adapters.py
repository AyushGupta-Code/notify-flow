"""SQS and Step Functions adapters against moto's in-process AWS mock.
These prove the adapter code paths and the ASL deploys; they are not a substitute for
running against real AWS, which this project has not done."""

import json
import os

import boto3
import pytest
from moto import mock_aws

from notify_flow.config import REPO_ROOT
from notify_flow.ingest import IngestHandler
from notify_flow.orchestrator.base import StartResult
from notify_flow.queue.sqs import SqsEventConsumer, SqsPublisher

pytestmark = pytest.mark.integration


@pytest.fixture
def aws():
    os.environ.update(
        AWS_ACCESS_KEY_ID="testing", AWS_SECRET_ACCESS_KEY="testing", AWS_DEFAULT_REGION="us-east-1"
    )
    with mock_aws():
        yield


class RecordingOrchestrator:
    name = "recording"

    def __init__(self):
        self.started = set()

    async def start_notification(self, inp):
        wid = f"notify-{inp.event_id}"
        new = wid not in self.started
        self.started.add(wid)
        return StartResult(wid, new)

    async def signal_engagement(self, workflow_id):
        pass


def event(eid="evt-sqs-000001"):
    return json.dumps(
        {
            "event_id": eid,
            "member_id": "m-9",
            "event_type": "session_missed",
            "occurred_at": "2026-11-20T15:00:00Z",
        }
    ).encode()


async def test_sqs_roundtrip_with_duplicate_delivery(aws, sm):
    sqs = boto3.client("sqs", region_name="us-east-1")
    dlq = sqs.create_queue(QueueName="member-events-dlq")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    url = sqs.create_queue(
        QueueName="member-events",
        Attributes={
            "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "5"})
        },
    )["QueueUrl"]
    pub = SqsPublisher(sqs, url)
    await pub.publish("m-9", event(), {"correlation_id": "cid-sqs"})
    await pub.publish("m-9", event(), {"correlation_id": "cid-sqs"})  # duplicate send

    orch = RecordingOrchestrator()
    handler = IngestHandler(sm, orch, "sqs", 60)
    outcomes = []

    async def h(msg):
        assert msg.key == "m-9" and msg.headers["correlation_id"] == "cid-sqs"
        outcomes.append(await handler.handle(msg))

    consumer = SqsEventConsumer(sqs, url, wait_s=0)
    handled = await consumer.poll_once(h)
    handled += await consumer.poll_once(h)
    assert handled == 2
    assert sorted(outcomes) == ["duplicate", "started"]
    assert orch.started == {"notify-evt-sqs-000001"}
    left = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages"])
    assert left["Attributes"]["ApproximateNumberOfMessages"] == "0"


async def test_sqs_failed_handler_leaves_message_for_redelivery(aws):
    sqs = boto3.client("sqs", region_name="us-east-1")
    url = sqs.create_queue(QueueName="q", Attributes={"VisibilityTimeout": "0"})["QueueUrl"]
    await SqsPublisher(sqs, url).publish("m", event(), {})

    async def boom(msg):
        raise RuntimeError("downstream down")

    consumer = SqsEventConsumer(sqs, url, wait_s=0)
    assert await consumer.poll_once(boom) == 0
    seen = []

    async def ok(msg):
        seen.append(msg)

    assert await consumer.poll_once(ok) == 1 and len(seen) == 1


async def test_step_functions_definition_registers_in_moto(aws):
    sfn = boto3.client("stepfunctions", region_name="us-east-1")
    iam = boto3.client("iam", region_name="us-east-1")
    role = iam.create_role(RoleName="sfn", AssumeRolePolicyDocument="{}")["Role"]["Arn"]
    definition = (REPO_ROOT / "aws/stepfunctions/notification.asl.json").read_text()
    for k in ("SelectChannel", "QuietHours", "Personalize", "Send", "Engagement", "Escalation"):
        definition = definition.replace(
            f"${{{k}FunctionArn}}", f"arn:aws:lambda:us-east-1:123456789012:function:nf-{k}"
        )
    arn = sfn.create_state_machine(name="notify-flow", definition=definition, roleArn=role)[
        "stateMachineArn"
    ]
    registered = sfn.describe_state_machine(stateMachineArn=arn)
    assert registered["name"] == "notify-flow"
    assert json.loads(registered["definition"])["StartAt"] == "SelectChannel"
