"""Output checks shared by the runtime guardrail and the offline eval harness."""

import re
from dataclasses import dataclass, field

from notify_flow.redaction import find_pii

PLACEHOLDER = "{first_name}"
SHOUTING_RE = re.compile(r"\b[A-Z]{5,}\b")
ALLOWED_CAPS = {"STOP", "HELP"}
UNFILLED_RE = re.compile(r"\{(?!first_name\})[a-z_]+\}|\[[A-Z_ ]+\]")


@dataclass
class CheckResult:
    passed: bool
    failures: list[str] = field(default_factory=list)


def check_copy(
    text: str,
    *,
    channel: str,
    limits: dict[str, int],
    banned: tuple[str, ...],
    member_values: list[str] | None = None,
) -> CheckResult:
    failures: list[str] = []
    rendered_len = len(text.replace(PLACEHOLDER, "Alexandra"))  # worst-case-ish name length
    if not text.strip():
        failures.append("empty")
    if rendered_len > limits.get(channel, 160):
        failures.append(f"length>{limits.get(channel, 160)}")
    if text.count(PLACEHOLDER) != 1:
        failures.append("placeholder_count")
    if UNFILLED_RE.search(text):
        failures.append("unfilled_variable")
    lowered = text.lower()
    hits = [p for p in banned if p in lowered]
    if hits:
        failures.append("banned_phrase")
    if text.count("!") > 1:
        failures.append("tone_exclamations")
    if any(w not in ALLOWED_CAPS for w in SHOUTING_RE.findall(text)):
        failures.append("tone_shouting")
    if find_pii(text, member_values):
        failures.append("pii")
    if channel == "email" and "opt out" not in lowered and "stop" not in lowered:
        failures.append("missing_opt_out")
    return CheckResult(passed=not failures, failures=failures)
