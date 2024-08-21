import datetime
import unittest

from analyzer_helper.telegram.utils.date_time_format_converter import (
    DateTimeFormatConverter,
)


class TestDateTimeFormatConverter(unittest.TestCase):
    def test_datetime_to_timestamp(self):
        dt = datetime.datetime(2023, 7, 1, tzinfo=datetime.timezone.utc)
        expected_timestamp = 1688169600.0
        self.assertEqual(
            DateTimeFormatConverter.datetime_to_timestamp(datetime=dt),
            expected_timestamp,
        )

    def test_timestamp_to_datetime(self):
        timestamp = 1688169600.0
        expected_datetime = datetime.datetime(
            2023, 7, 1, 0, 0, tzinfo=datetime.timezone.utc
        )
        self.assertEqual(
            DateTimeFormatConverter.timestamp_to_datetime(timestamp=timestamp),
            expected_datetime,
        )
