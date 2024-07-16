from datetime import datetime, timezone

class DateTimeFormatConverter:
    @staticmethod
    def to_iso_format(dt: datetime) -> str:
        """
        Converts a datetime object to an ISO 8601 formatted string with milliseconds and 'Z' suffix.
        
        Args:
            dt (datetime): The datetime object to convert.
            
        Returns:
            str: The ISO 8601 formatted string.
        """
        return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    
    @staticmethod
    def convert_to_datetime(date_string: str) -> datetime:
        """
        Convert an ISO 8601 formatted date string to a Python datetime object in UTC.
        
        Args:
            date_string (str): ISO 8601 formatted date string.
            
        Returns:
            datetime: The Python datetime object in UTC.
        """
        datetime_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        dt = datetime.strptime(date_string, datetime_format)
        dt = dt.replace(tzinfo=timezone.utc)
        return dt