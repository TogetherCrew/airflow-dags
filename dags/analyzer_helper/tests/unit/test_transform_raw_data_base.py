import unittest
from datetime import datetime

from analyzer_helper.discord.transform_raw_data_base import TransformRawDataBase


class TestTransformRawDataBase(unittest.TestCase):
    def setUp(self):
        self.raw_data = []
        self.platform_id = "test_platform"
        self.period = datetime.now()
        self.test_instance = TransformRawDataBase()

    def test_transform_abstract(self):
        """
        Tests that the transform method is abstract and raises a NotImplementedError
        """
        with self.assertRaises(NotImplementedError):
            self.test_instance.transform(self.raw_data, self.platform_id, self.period)
