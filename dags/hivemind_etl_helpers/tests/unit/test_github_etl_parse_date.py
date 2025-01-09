import logging
import unittest
from datetime import datetime

from hivemind_etl_helpers.src.db.github.schema.utils import parse_date_variable
from neo4j.time import DateTime


class TestParseDateVariables(unittest.TestCase):
    def test_valid_date_string(self):
        self.assertEqual(parse_date_variable("2024-02-26"), 1708905600.0)

    def test_valid_datetime_object(self):
        self.assertEqual(parse_date_variable(datetime(2024, 2, 26)), 1708893000.0)

    def test_valid_neo4j_datetime_object(self):
        self.assertEqual(
            parse_date_variable(DateTime(2024, 2, 26, 12, 30, 45)),
            1708950645.0,
        )

    def test_invalid_date_type(self):
        with self.assertLogs(level=logging.WARNING):
            self.assertEqual(parse_date_variable(123), 123)

    def test_invalid_date_string(self):
        self.assertEqual(parse_date_variable("2024-02-20"), 1708387200.0)

    def test_edge_case_none_input(self):
        self.assertIsNone(parse_date_variable(None))
