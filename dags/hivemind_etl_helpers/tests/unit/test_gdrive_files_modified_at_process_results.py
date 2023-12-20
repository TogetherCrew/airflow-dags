from datetime import datetime
from unittest import TestCase

from hivemind_etl_helpers.src.db.gdrive.db_utils import postprocess_results


class TestProcessGdriveFileRetreivalResultsProcess(TestCase):
    def test_empty_list(self):
        self.assertEqual(postprocess_results([]), {})

    def test_single_dict(self):
        input_data = [{"key1": "2023-09-26T22:29:17"}]
        expected_output = {"key1": datetime(2023, 9, 26, 22, 29, 17)}
        self.assertEqual(postprocess_results(input_data), expected_output)

    def test_multiple_dicts(self):
        input_data = [
            {"key1": "2023-09-26T22:29:17"},
            {"key2": "2022-08-15T14:30:00"},
            {"key3": "2021-05-10T08:45:30"},
        ]
        expected_output = {
            "key1": datetime(2023, 9, 26, 22, 29, 17),
            "key2": datetime(2022, 8, 15, 14, 30, 0),
            "key3": datetime(2021, 5, 10, 8, 45, 30),
        }
        self.assertEqual(postprocess_results(input_data), expected_output)

    def test_empty_dicts(self):
        input_data = [{}, {}, {}]
        expected_output = {}
        self.assertEqual(postprocess_results(input_data), expected_output)

    def test_mixed_dicts(self):
        input_data = [
            {"key1": "2023-09-26T22:29:17"},
            {},
            {"key2": "2022-08-15T14:30:00"},
            {},
            {"key3": "2021-05-10T08:45:30"},
        ]
        expected_output = {
            "key1": datetime(2023, 9, 26, 22, 29, 17),
            "key2": datetime(2022, 8, 15, 14, 30, 0),
            "key3": datetime(2021, 5, 10, 8, 45, 30),
        }
        self.assertEqual(postprocess_results(input_data), expected_output)
