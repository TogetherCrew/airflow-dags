import datetime
from datetime import datetime, timezone


class DateTimeFormatConverter:
    @staticmethod
    def datetime_to_timestamp(datetime: datetime) -> float:
        """
        Convert a Python datetime object to a Unix timestamp.

        :param datetime: The datetime object to convert.
        :return: The corresponding Unix timestamp.
        """
        return datetime.replace(tzinfo=timezone.utc).timestamp()

    @staticmethod
    def timestamp_to_datetime(timestamp: float) -> datetime:
        """
        Convert a Unix timestamp to a Python datetime object.

        :param timestamp: The Unix timestamp to convert.
        :return: The corresponding datetime object.
        """
        return datetime.fromtimestamp(timestamp, timezone.utc)

    @staticmethod
    def string_to_datetime(date_string: str) -> datetime:
        """
        Convert a date string to a Python datetime object.
        :param date_string: The date string to convert.
        :return: The corresponding datetime object.
        """
        return datetime.fromisoformat(date_string)
