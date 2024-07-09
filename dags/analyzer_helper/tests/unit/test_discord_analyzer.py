import unittest
from datetime import datetime

from analyzer_helper.discord.discord_analyze import Analyzer
import pytest


@pytest.mark.skip("The Analyzer lib is fully test before!")
class TestAnalyzer(unittest.TestCase):
    def test_analyze_abstract(self):
        """
        Tests that the analyze method is abstract and raises a NotImplementedError
        """
        analyzer = Analyzer()
        with self.assertRaises(NotImplementedError):
            analyzer.analyze(
                "test_platform",
                ["channel1", "channel2"],
                datetime.now(),
                {"INT_THR": 1, "UW_DEG_THR": 1},
                {"period_size": 7, "step_size": 1},
            )
