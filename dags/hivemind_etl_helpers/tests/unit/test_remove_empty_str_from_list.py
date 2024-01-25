import unittest

from hivemind_etl_helpers.src.db.discord.utils.content_parser import remove_empty_str


class TestRemoveEmptyStr(unittest.TestCase):
    def test_remove_empty_str_basic(self):
        input_data = ["apple", "", "banana", "", "cherry"]
        expected_output = ["apple", "banana", "cherry"]
        self.assertEqual(remove_empty_str(input_data), expected_output)

    def test_remove_empty_str_no_empty_strings(self):
        input_data = ["apple", "banana", "cherry"]
        expected_output = ["apple", "banana", "cherry"]
        self.assertEqual(remove_empty_str(input_data), expected_output)

    def test_remove_empty_str_empty_list(self):
        input_data = []
        expected_output = []
        self.assertEqual(remove_empty_str(input_data), expected_output)

    def test_remove_empty_str_all_empty_strings(self):
        input_data = ["", "", ""]
        expected_output = []
        self.assertEqual(remove_empty_str(input_data), expected_output)

    def test_remove_empty_str_single_empty_string(self):
        input_data = [""]
        expected_output = []
        self.assertEqual(remove_empty_str(input_data), expected_output)

    def test_remove_empty_str_multiple_occurrences(self):
        input_data = ["", "apple", "", "", "banana", "", "cherry", ""]
        expected_output = ["apple", "banana", "cherry"]
        self.assertEqual(remove_empty_str(input_data), expected_output)
