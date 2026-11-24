from datetime import UTC, datetime

from notify_flow.quiet_hours import next_allowed_send


def test_outside_quiet_hours_sends_now():
    now = datetime(2026, 6, 1, 16, 0, tzinfo=UTC)  # 12:00 New York
    assert next_allowed_send(now, "America/New_York", 21, 8) == now


def test_late_evening_waits_until_8am_next_day():
    now = datetime(2026, 6, 2, 2, 30, tzinfo=UTC)  # 22:30 New York, June 1
    got = next_allowed_send(now, "America/New_York", 21, 8)
    assert got == datetime(2026, 6, 2, 12, 0, tzinfo=UTC)  # 08:00 EDT


def test_early_morning_waits_until_8am_same_day():
    now = datetime(2026, 6, 1, 13, 0, tzinfo=UTC)  # 06:00 Los Angeles
    got = next_allowed_send(now, "America/Los_Angeles", 21, 8)
    assert got == datetime(2026, 6, 1, 15, 0, tzinfo=UTC)


def test_non_wrapping_window():
    now = datetime(2026, 6, 1, 13, 0, tzinfo=UTC)  # 13:00 UTC
    assert next_allowed_send(now, "UTC", 12, 14) == datetime(2026, 6, 1, 14, 0, tzinfo=UTC)
