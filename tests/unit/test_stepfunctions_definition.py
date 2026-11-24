import json

from notify_flow.config import REPO_ROOT


def definition():
    return json.loads((REPO_ROOT / "aws/stepfunctions/notification.asl.json").read_text())


def test_every_task_has_timeout_jittered_retry_and_catch():
    tasks = [state for state in definition()["States"].values() if state["Type"] == "Task"]
    assert len(tasks) == 8
    for task in tasks:
        assert task["TimeoutSeconds"] > 0
        assert task["Retry"][0]["BackoffRate"] > 1
        assert task["Retry"][0]["JitterStrategy"] == "FULL"
        assert task["Catch"][0]["Next"] == "CaughtFailure"


def test_definition_has_quiet_wait_and_engagement_choice():
    states = definition()["States"]
    assert states["WaitForQuietHours"]["SecondsPath"] == "$.quiet_wait_seconds"
    assert states["WaitForEngagementWindow"]["SecondsPath"] == "$.engagement_wait_s"
    assert states["WasEngaged"]["Choices"][0] == {
        "Variable": "$.engaged",
        "BooleanEquals": True,
        "Next": "Completed",
    }
    assert any(
        choice.get("StringEqualsPath") == "$.last_send.channel"
        for choice in states["CanEscalate"]["Choices"]
    )


def test_definition_references_all_lambda_handlers():
    raw = (REPO_ROOT / "aws/stepfunctions/notification.asl.json").read_text()
    assert {
        "${SelectChannelFunctionArn}",
        "${QuietHoursFunctionArn}",
        "${PersonalizeFunctionArn}",
        "${SendFunctionArn}",
        "${EngagementFunctionArn}",
        "${EscalationFunctionArn}",
    } <= set(part for part in raw.replace('"', " ").replace(",", " ").split() if "${" in part)


def test_lambda_tasks_return_handler_payload_and_preserve_catch_input():
    tasks = [state for state in definition()["States"].values() if state["Type"] == "Task"]
    assert all(task["Resource"].startswith("${") for task in tasks)
    assert all("OutputPath" not in task and "Parameters" not in task for task in tasks)
