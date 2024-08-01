import datetime


class DateTimeFormatConverter:
    @staticmethod
    def to_iso_format(dt: datetime.datetime) -> str:
        """
        Convert datetime to ISO format string with milliseconds.

        :param dt: The datetime object to convert.
        :return: ISO format string.
        """
        return dt.isoformat() + "Z"

    @staticmethod
    def from_iso_format(iso_string_or_datetime):
        """
        Convert ISO format string or datetime object to datetime object.

        :param iso_string_or_datetime: ISO format string or datetime object.
        :return: datetime object.
        """
        if isinstance(iso_string_or_datetime, datetime.datetime):
            return iso_string_or_datetime
        elif isinstance(iso_string_or_datetime, str):
            return datetime.datetime.fromisoformat(
                iso_string_or_datetime.replace("Z", "+00:00")
            )
        else:
            raise TypeError(
                f"Expected string or datetime, got {type(iso_string_or_datetime)}"
            )

    @staticmethod
    def from_date_string(
        date_string: str,
        hour: int = 0,
        minute: int = 0,
        second: int = 0,
        microsecond: int = 0,
    ) -> datetime.datetime:
        """
        Convert a date string to a datetime object.

        :param date_string: Date string in the format 'YYYY-MM-DD'.
        :param hour: Hour component of the time.
        :param minute: Minute component of the time.
        :param second: Second component of the time.
        :param microsecond: Microsecond component of the time.
        :return: datetime object.
        """
        date = datetime.datetime.strptime(date_string, "%Y-%m-%d")
        return datetime.datetime(
            date.year, date.month, date.day, hour, minute, second, microsecond
        )
