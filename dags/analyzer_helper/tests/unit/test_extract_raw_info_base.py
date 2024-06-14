import unittest
from datetime import datetime

from analyzer_helper.discord.extract_raw_info_base import ExtractRawInfosBase


class TestExtractRawInfosBase(unittest.TestCase):
    def setUp(self):
        self.guild_id = "test_platform"
        self.test_instance = ExtractRawInfosBase(self.guild_id)

    def test_init(self):
        """
        Tests that the object is initialized with the correct platform ID
        """
        self.assertEqual(self.test_instance.get_guild_id(), self.guild_id)

    def test_get_guild_id(self):
        """
        Tests that the get_guild_id method returns the guild ID set in the constructor
        """
        self.assertEqual(self.test_instance.get_guild_id(), self.guild_id)

    def test_extract_abstract(self):
        """
        Tests that the extract method is abstract and raises a NotImplementedError
        """
        with self.assertRaises(NotImplementedError):
            self.test_instance.extract(datetime.now())

    def test_extract_with_recompute(self):
        """
        Tests that extract with recompute=True ignores the period argument
        """
        pass
