from datetime import datetime, timezone

# Previous

# class DateTimeFormatConverter:
#     @staticmethod
#     def to_iso_format(dt: datetime) -> str:
#         """
#         Converts a datetime object to an ISO 8601 formatted string with milliseconds and 'Z' suffix.
        
#         Args:
#             dt (datetime): The datetime object to convert.
            
#         Returns:
#             str: The ISO 8601 formatted string.
#         """
#         return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    
#     @staticmethod
#     def convert_to_datetime(date_string: str) -> datetime:
#         """
#         Convert an ISO 8601 formatted date string to a Python datetime object in UTC.
        
#         Args:
#             date_string (str): ISO 8601 formatted date string.
            
#         Returns:
#             datetime: The Python datetime object in UTC.
#         """
#         datetime_format = "%Y-%m-%dT%H:%M:%S.%fZ"
#         dt = datetime.strptime(date_string, datetime_format)
#         dt = dt.replace(tzinfo=timezone.utc)
#         return dt


# Hopefully final
# class DateTimeFormatConverter:
#     @staticmethod
#     def to_iso_format(dt: datetime) -> str:
#         """
#         Convert datetime to ISO format string with milliseconds.

#         :param dt: The datetime object to convert.
#         :return: ISO format string.
#         """
#         return dt.isoformat() + 'Z'

#     @staticmethod
#     def from_iso_format(iso_string: str) -> datetime:
#         """
#         Convert ISO format string to datetime object.

#         :param iso_string: ISO format string.
#         :return: datetime object.
#         """
#         return datetime.fromisoformat(iso_string.replace('Z', '+00:00'))


class DateTimeFormatConverter:
    @staticmethod
    def to_iso_format(dt: datetime) -> str:
        """
        Convert datetime to ISO format string with milliseconds.

        :param dt: The datetime object to convert.
        :return: ISO format string.
        """
        return dt.isoformat() + 'Z'

    @staticmethod
    def from_iso_format(iso_string: str) -> datetime:
        """
        Convert ISO format string to datetime object.

        :param iso_string: ISO format string.
        :return: datetime object.
        """
        return datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
    
    @staticmethod
    def from_date_string(date_string: str, hour: int = 0, minute: int = 0, second: int = 0, microsecond: int = 0) -> datetime:
        """
        Convert a date string to a datetime object.

        :param date_string: Date string in the format 'YYYY-MM-DD'.
        :param hour: Hour component of the time.
        :param minute: Minute component of the time.
        :param second: Second component of the time.
        :param microsecond: Microsecond component of the time.
        :return: datetime object.
        """
        date = datetime.strptime(date_string, '%Y-%m-%d')
        return datetime(date.year, date.month, date.day, hour, minute, second, microsecond)
