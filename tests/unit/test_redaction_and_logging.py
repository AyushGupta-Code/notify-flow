import json

from notify_flow.log import configure_logging, correlation_id, get_logger
from notify_flow.redaction import find_pii, redact, redact_text


def test_redact_text_masks_email_phone_ssn_dates():
    out = redact_text("mail jo@example.com or call (415) 555-0199, ssn 123-45-6789, dob 1980-02-03")
    assert "example.com" not in out and "555-0199" not in out
    assert "123-45-6789" not in out and "1980-02-03" not in out
    assert "[REDACTED_EMAIL]" in out and "[REDACTED_PHONE]" in out


def test_redact_dict_masks_sensitive_keys_recursively():
    out = redact({"member": {"first_name": "Jo", "email": "a@b.co", "program_week": 3}})
    assert out == {"member": {"first_name": "[REDACTED]", "email": "[REDACTED]", "program_week": 3}}


def test_find_pii_member_values():
    assert find_pii("Hi Priya, see you", ["Priya"]) == ["member_value"]
    assert find_pii("Hi {first_name}", ["Priya"]) == []


def test_json_log_has_correlation_id_and_no_pii(capsys):
    configure_logging("INFO", "test")
    correlation_id.set("cid-123")
    get_logger().info(
        "member.updated", email="jo@example.com", note="call 415-555-0199", program_week=4
    )
    line = capsys.readouterr().out.strip().splitlines()[-1]
    rec = json.loads(line)
    assert rec["correlation_id"] == "cid-123"
    assert rec["msg"] == "member.updated"
    assert rec["email"] == "[REDACTED]"
    assert "555-0199" not in line
    assert rec["program_week"] == 4
    correlation_id.set(None)
