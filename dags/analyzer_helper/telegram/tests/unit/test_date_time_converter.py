import datetime
import unittest

from analyzer_helper.telegram.utils.date_time_format_converter import (
    DateTimeFormatConverter,
)


class TestDateTimeFormatConverter(unittest.TestCase):
    def test_datetime_to_timestamp(self):
        datetime = datetime.datetime(2023, 7, 1)
        expected_timestamp = 1688179200.0
        self.assertEqual(DateTimeFormatConverter.datetime_to_timestamp(datetime=datetime), expected_timestamp)
    
    def test_timestamp_to_datetime(self):
        timestamp = 1688179200.0
        expected_datetime = datetime.datetime(2023, 7, 1)
        self.assertEqual(DateTimeFormatConverter.timestamp_to_datetime(timestamp=timestamp), expected_datetime)