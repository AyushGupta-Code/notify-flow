from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


def next_allowed_send(now_utc: datetime, tz: str, start_hour: int, end_hour: int) -> datetime:
    """Earliest time at or after now_utc that is outside the member's local quiet hours
    [start_hour, end_hour), which may wrap midnight (e.g. 21 -> 8)."""
    local = now_utc.astimezone(ZoneInfo(tz))
    h = local.hour
    in_quiet = (
        (start_hour <= h or h < end_hour) if start_hour > end_hour else (start_hour <= h < end_hour)
    )
    if not in_quiet:
        return now_utc
    target = local.replace(hour=end_hour, minute=0, second=0, microsecond=0)
    if target <= local:
        target += timedelta(days=1)
    return target.astimezone(now_utc.tzinfo)
