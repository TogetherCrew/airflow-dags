import unittest
from datetime import datetime
import logging
from neo4j.time import DateTime
from hivemind_etl_helpers.src.db.github.schema.utils import (
    parse_date_variables,
)


class TestParseDateVariables(unittest.TestCase):
    def test_valid_date_string(self):
        self.assertEqual(parse_date_variables("2024-02-26"), "2024-02-26 00:00:00")

    def test_valid_datetime_object(self):
        self.assertEqual(
            parse_date_variables(datetime(2024, 2, 26)), "2024-02-26 00:00:00"
        )

    def test_valid_neo4j_datetime_object(self):
        self.assertEqual(
            parse_date_variables(DateTime(2024, 2, 26, 12, 30, 45)),
            "2024-02-26 12:30:45",
        )

    def test_invalid_date_type(self):
        with self.assertLogs(level=logging.WARNING):
            self.assertEqual(parse_date_variables(123), 123)

    def test_invalid_date_string(self):
        self.assertEqual(parse_date_variables("2024-02-20"), "2024-02-20 00:00:00")

    def test_edge_case_none_input(self):
        self.assertIsNone(parse_date_variables(None))
