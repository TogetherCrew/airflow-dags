import unittest

from analyzer_helper.discord.extract_raw_member_base import ExtractRawMembersBase


class TestExtractRawMembersBase(unittest.TestCase):

    def setUp(self):
        self.platform_id = "test_platform"
        self.test_instance = ExtractRawMembersBase(self.platform_id)

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

    def test_extract_abstract(self):
        """
        Tests that the extract method is abstract and raises a NotImplementedError
        """
        with self.assertRaises(NotImplementedError):
            self.test_instance.extract()
