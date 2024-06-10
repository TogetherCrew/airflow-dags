import unittest

from analyzer_helper.discord.load_transformed_data_base import LoadTransformedDataBase


class TestLoadTransformedDataBase(unittest.TestCase):

    def setUp(self):
        self.platform_id = 'test_platform'
        self.loader = LoadTransformedDataBase(self.platform_id)

    def test_init(self):
        """
        Test that the platform_id is correctly set during initialization
        """
        self.assertEqual(self.loader._platform_id, self.platform_id)

    def test_get_platform_id(self):
        """
        Test that get_platform_id returns the correct platform ID
        """
        self.assertEqual(self.loader.get_platform_id(), self.platform_id)

    def test_load_not_implemented(self):
        """
        Test that the load method is not implemented
        """
        with self.assertRaises(NotImplementedError):
            self.loader.load([])
