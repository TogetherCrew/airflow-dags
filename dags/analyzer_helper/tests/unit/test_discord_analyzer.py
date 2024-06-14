import unittest

from analyzer_helper.discord.discord_analyze import Analyzer


class TestAnalyzer(unittest.TestCase):
    def test_analyze_abstract(self):
        """
        Tests that the analyze method is abstract and raises a NotImplementedError
        """
        analyzer = Analyzer()
        with self.assertRaises(NotImplementedError):
            analyzer.analyze("test_platform", ["channel1", "channel2"])
