"""PII redaction for logs and LLM prompts.

Data in this project is synthetic, but the code treats it as PHI: no direct identifier
reaches a log line or an LLM prompt.
"""

import re
from typing import Any

SENSITIVE_KEYS = {
    "email",
    "phone",
    "first_name",
    "last_name",
    "name",
    "dob",
    "date_of_birth",
    "address",
    "ssn",
    "body",  # rendered message bodies contain the member's name
}

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(?:\+?1[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}")
SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
DATE_RE = re.compile(r"\b(?:19|20)\d{2}-\d{2}-\d{2}\b")

PATTERNS = (("email", EMAIL_RE), ("ssn", SSN_RE), ("phone", PHONE_RE), ("date", DATE_RE))


def redact_text(text: str) -> str:
    for label, pattern in PATTERNS:
        text = pattern.sub(f"[REDACTED_{label.upper()}]", text)
    return text


def find_pii(text: str, extra_values: list[str] | None = None) -> list[str]:
    """Return the kinds of PII found in text. extra_values are member-specific strings
    (name, email, ...) that must never appear verbatim."""
    found = [label for label, pattern in PATTERNS if pattern.search(text)]
    lowered = text.lower()
    for value in extra_values or []:
        if value and len(value) > 1 and value.lower() in lowered:
            found.append("member_value")
    return found


def redact(value: Any, key: str | None = None) -> Any:
    if key is not None and key.lower() in SENSITIVE_KEYS:
        return "[REDACTED]"
    if isinstance(value, dict):
        return {k: redact(v, k) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [redact(v) for v in value]
    if isinstance(value, str):
        return redact_text(value)
    return value
