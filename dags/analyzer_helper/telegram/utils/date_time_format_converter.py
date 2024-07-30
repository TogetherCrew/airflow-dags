import datetime


class DateTimeFormatConverter:
    @staticmethod
    def datetime_to_timestamp(datetime: datetime.datetime) -> float:
        """
        Convert a Python datetime object to a Unix timestamp.
        
        :param datetime: The datetime object to convert.
        :return: The corresponding Unix timestamp.
        """
        return datetime.timestamp()
    @staticmethod
    def timestamp_to_datetime(timestamp: float) -> datetime.datetime:
        """
        Convert a Unix timestamp to a Python datetime object.
        
        :param timestamp: The Unix timestamp to convert.
        :return: The corresponding datetime object.
        """
        return datetime.datetime.fromtimestamp(timestamp)
