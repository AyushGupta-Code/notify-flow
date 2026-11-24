import json

import pytest
from pydantic import ValidationError

from notify_flow.config import REPO_ROOT
from notify_flow.events import MemberEvent, campaign_for


def ev(t, **payload):
    return MemberEvent(event_id="evt-00000001", member_id="m1", event_type=t, payload=payload)


def test_campaign_mapping():
    assert campaign_for(ev("enrolled")) == "welcome"
    assert campaign_for(ev("session_missed")) == "reengage"
    assert campaign_for(ev("session_completed", session_count=10)) == "milestone"
    assert campaign_for(ev("session_completed", session_count=7)) is None
    assert campaign_for(ev("pain_score_reported", score=8)) == "pain_checkin"
    assert campaign_for(ev("pain_score_reported", score=3)) is None


def test_schema_rejects_unknown_version_and_type():
    with pytest.raises(ValidationError):
        MemberEvent(
            event_id="evt-00000001", member_id="m1", event_type="enrolled", schema_version=2
        )
    with pytest.raises(ValidationError):
        MemberEvent(event_id="evt-00000001", member_id="m1", event_type="hacked")


def test_step_functions_definition_is_well_formed():
    asl = json.loads((REPO_ROOT / "aws/stepfunctions/notification.asl.json").read_text())
    states = asl["States"]
    assert asl["StartAt"] in states

    def targets(state):
        out = [state.get("Next"), state.get("Default")]
        out += [c.get("Next") for c in state.get("Choices", [])]
        out += [c.get("Next") for c in state.get("Catch", [])]
        return [t for t in out if t]

    terminal = [n for n, s in states.items() if s.get("End") or s["Type"] in ("Succeed", "Fail")]
    assert terminal
    for name, s in states.items():
        for t in targets(s):
            assert t in states, f"{name} -> {t} missing"
        if s["Type"] == "Task":
            assert s.get("Retry"), f"{name} has no Retry"
    # The durable wait is followed by a real engagement task and a Choice branch.
    wait = states["WaitForEngagementWindow"]
    assert wait["Type"] == "Wait" and wait["SecondsPath"] == "$.engagement_wait_s"
    assert wait["Next"] == "CheckEngagement"
    assert states["CheckEngagement"]["Next"] == "WasEngaged"
