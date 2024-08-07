import datetime
import unittest

from analyzer_helper.discourse.utils.convert_date_time_formats import (
    DateTimeFormatConverter,
)


class TestDateTimeFormatConverter(unittest.TestCase):
    def test_to_iso_format(self):
        dt = datetime.datetime(2023, 7, 1, 16, 57, 46, 149000)
        iso_string = DateTimeFormatConverter.to_iso_format(dt)
        self.assertEqual(iso_string, "2023-07-01T16:57:46.149000Z")

    def test_from_iso_format(self):
        iso_string = "2023-07-01T16:57:46.149Z"
        dt = DateTimeFormatConverter.from_iso_format(iso_string)
        expected_dt = datetime.datetime(
            2023, 7, 1, 16, 57, 46, 149000, tzinfo=datetime.timezone.utc
        )
        self.assertEqual(dt, expected_dt)

    def test_from_date_string(self):
        date_string = "2023-07-01"
        dt = DateTimeFormatConverter.from_date_string(date_string)
        self.assertEqual(
            dt, datetime.datetime(2023, 7, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)
        )
        dt_with_time = DateTimeFormatConverter.from_date_string(date_string)
        expected_time = datetime.datetime(2023, 7, 1, tzinfo=datetime.timezone.utc)
        self.assertEqual(dt_with_time, expected_time)
