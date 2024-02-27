from datetime import datetime
import logging

from neo4j.time import DateTime
from dateutil import parser
from hivemind_etl_helpers.src.db.globals import DATE_FORMAT


def parse_date_variables(date: datetime | str) -> str:
    """
    parse date variables into a global string format
    """
    date_parsed: str
    if isinstance(date, str):
        date_parsed = parser.parse(date).strftime(DATE_FORMAT)
    elif isinstance(date, datetime):
        date_parsed = date.strftime(DATE_FORMAT)
    elif isinstance(date, DateTime):
        date_parsed = datetime(
            year=date.year,
            month=date.month,
            day=date.day,
            hour=date.hour,
            minute=date.minute,
            second=date.second,
        ).strftime(DATE_FORMAT)
    else:
        logging.warning(
            f"date is in format {type(date)} and cannot be parsed! "
            "The return value would be the same value."
        )
        date_parsed = date

    return date_parsed
