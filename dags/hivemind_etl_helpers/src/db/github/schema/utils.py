import logging
from datetime import datetime, timezone

from dateutil import parser
from neo4j.time import DateTime


def parse_date_variable(date: datetime | str) -> float:
    """
    parse date variables into a float timestamp format
    """
    date_parsed: str
    if isinstance(date, str):
        date_parsed = parser.parse(date).replace(tzinfo=timezone.utc).timestamp()
    elif isinstance(date, datetime):
        date_parsed = date.replace(tzinfo=timezone.utc).timestamp()
    elif isinstance(date, DateTime):
        date_parsed = (
            datetime(
                year=date.year,
                month=date.month,
                day=date.day,
                hour=date.hour,
                minute=date.minute,
                second=date.second,
            )
            .replace(tzinfo=timezone.utc)
            .timestamp()
        )
    else:
        logging.warning(
            f"date is in format {type(date)} and cannot be parsed! "
            "The return value would be the same value."
        )
        date_parsed = date

    return date_parsed
