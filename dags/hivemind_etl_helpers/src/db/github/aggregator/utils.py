from datetime import datetime, timezone


def get_day_timestamp(timestamp: float) -> float:
    """Convert a timestamp to start of day timestamp in UTC."""
    date = datetime.fromtimestamp(timestamp, tz=timezone.utc).date()
    return datetime.combine(date, datetime.min.time(), tzinfo=timezone.utc).timestamp()
