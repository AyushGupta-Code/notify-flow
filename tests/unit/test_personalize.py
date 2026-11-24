import pytest

from notify_flow.config import REPO_ROOT
from notify_flow.llm.guardrails import check_copy
from notify_flow.llm.personalize import Personalizer
from notify_flow.llm.providers import LLMError, MockProvider
from notify_flow.llm.registry import load_registry
from notify_flow.llm.resilience import CircuitBreaker

REG = load_registry(REPO_ROOT / "prompts" / "registry.yaml")


class FailingProvider:
    name, model = "failing", "fail-1"

    def __init__(self):
        self.calls = 0

    async def generate(self, prompt, max_tokens=200):
        self.calls += 1
        raise LLMError("down")


class FixedProvider:
    name, model = "fixed", "fixed-1"

    def __init__(self, text):
        self.text = text
        self.prompts = []

    async def generate(self, prompt, max_tokens=200):
        self.prompts.append(prompt)
        return self.text


def make(provider, threshold=5):
    return Personalizer(
        REG, provider, CircuitBreaker(threshold, 30), timeout_s=1, max_attempts=2, backoff_base_s=0
    )


async def test_llm_copy_is_versioned():
    r = await make(MockProvider()).compose("reengage", "sms", {"program_week": 3})
    assert r.source == "llm" and r.reason == "ok"
    assert r.prompt_version == "reengage-v2" and r.llm_model == "mock-copywriter-1"
    assert r.template_version == "tmpl-reengage-v1"
    assert r.registry_version == REG.registry_version


async def test_llm_failure_falls_back_to_template():
    p = FailingProvider()
    r = await make(p).compose("reengage", "push", {})
    assert r.source == "template" and r.reason == "llm_error" and p.calls == 2
    assert "{first_name}" in r.body_template


async def test_breaker_opens_and_stops_calling_provider():
    p = FailingProvider()
    pers = make(p, threshold=2)
    await pers.compose("reengage", "push", {})  # 2 attempts -> breaker opens
    calls_before = p.calls
    r = await pers.compose("reengage", "push", {})
    assert r.reason == "circuit_open" and p.calls == calls_before


async def test_guardrail_rejects_pii_and_uses_template():
    p = FixedProvider("Hi {first_name}, call us at 415-555-0100 to talk.")
    r = await make(p).compose("reengage", "sms", {})
    assert r.source == "template" and r.reason == "guardrail"


async def test_guardrail_rejects_member_name_in_output():
    p = FixedProvider("Hi {first_name}! Priya, come back.")
    r = await make(p).compose("reengage", "sms", {}, member_values=["Priya"])
    assert r.reason == "guardrail"


async def test_clinical_campaign_never_calls_llm():
    p = FixedProvider("unused")
    r = await make(p).compose("pain_checkin", "sms", {})
    assert r.source == "template" and r.reason == "policy" and p.prompts == []


async def test_prompt_only_contains_whitelisted_context():
    p = FixedProvider("Hi {first_name}, a short session today still counts.")
    await make(p).compose(
        "reengage",
        "sms",
        {
            "program_week": 3,
            "days_since_last_session": 9,
            "first_name": "Priya",
            "email": "p@x.io",
            "diagnosis": "lumbar",
        },
    )
    prompt = p.prompts[0]
    assert "Priya" not in prompt and "p@x.io" not in prompt and "lumbar" not in prompt
    assert '"days_since_last_session": 9' in prompt


@pytest.mark.parametrize("campaign", list(REG.campaigns))
@pytest.mark.parametrize("channel", ["sms", "push", "email"])
def test_every_fallback_template_passes_guardrails(campaign, channel):
    c = REG.campaigns[campaign]
    body = (c.templates.get(channel) or c.templates["default"]).replace("{session_count}", "10")
    res = check_copy(body, channel=channel, limits=REG.channel_limits, banned=REG.banned_phrases)
    assert res.passed, res.failures
