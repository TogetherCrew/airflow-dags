import unittest
from analyzer_helper.discord.transform_raw_members_base import TransformRawMembersBase


class TestTransformRawMembersBase(unittest.TestCase):

    def setUp(self):
        self.test_instance = TransformRawMembersBase()

    def test_transform_abstract(self):
        """
        Tests that the transform method is abstract and raises a NotImplementedError
        """
        with self.assertRaises(NotImplementedError):
            self.test_instance.transform()
