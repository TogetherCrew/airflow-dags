import unittest
from unittest.mock import patch

from dags.analyzer_helper.discord.load_transformed_data_base import LoadTransformedDataBase


class TestLoadTransformedDataBase(unittest.TestCase):

    def setUp(self):
        self.platform_id = "test_platform"
        self.processed_data = []
        self.test_instance = LoadTransformedDataBase(self.platform_id)

    def test_init(self):
        """
        Tests that the object is initialized with the correct platform ID
        """
        self.assertEqual(self.test_instance.get_platform_id(), self.platform_id)

    def test_get_platform_id(self):
        """
        Tests that the get_platform_id method returns the platform ID set in the constructor
        """
        self.assertEqual(self.test_instance.get_platform_id(), self.platform_id)

    @patch('from dags.analyzer_helper.discord.load_transformed_data_base.LoadTransformedDataBase')
    def test_load(self, mock_load_impl):
        """
        Tests that the load method calls the load_impl method with the correct arguments
        """
        self.test_instance.load(self.processed_data)
        mock_load_impl.assert_called_once_with(self.processed_data, self.platform_id, False)

    @patch('from dags.analyzer_helper.discord.load_transformed_data_base.LoadTransformedDataBase')
    def test_load_with_recompute(self, mock_load_impl):
        """
        Tests that the load method calls the load_impl method with recompute=True when specified
        """
        self.test_instance.load(self.processed_data, recompute=True)
        mock_load_impl.assert_called_once_with(self.processed_data, self.platform_id, True)
