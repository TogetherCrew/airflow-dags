import unittest

from hivemind_etl_helpers.src.db.github.schema import GitHubIssue
from hivemind_etl_helpers.src.db.github.aggregator import IssueAggregator


class TestIssueAggregator(unittest.TestCase):
    def setUp(self):
        self.aggregator = IssueAggregator()

        self.issue1 = GitHubIssue(
            id=1,
            author_name="user1",
            title="Test Issue 1",
            text="Issue description 1",
            state="open",
            state_reason=None,
            created_at="2024-01-01 10:00:00",
            updated_at="2024-01-01 11:00:00",
            latest_saved_at="2024-01-01 12:00:00",
            url="http://example.com/1",
            repository_id="1",
            repository_name="repo1",
        )

        self.issue2 = GitHubIssue(
            id=2,
            author_name="user2",
            title="Test Issue 2",
            text="Issue description 2",
            state="closed",
            state_reason="completed",
            created_at="2024-01-01 13:00:00",
            updated_at="2024-01-01 14:00:00",
            latest_saved_at="2024-01-01 15:00:00",
            url="http://example.com/2",
            repository_id="1",
            repository_name="repo1",
        )

    def test_add_single_issue(self):
        self.aggregator.add_issue(self.issue1)
        daily_issues = self.aggregator.get_daily_issues("2024-01-01")
        self.assertEqual(len(daily_issues["2024-01-01"]), 1)
        self.assertEqual(daily_issues["2024-01-01"][0].id, 1)

    def test_add_multiple_issues(self):
        issues = [self.issue1, self.issue2]
        self.aggregator.add_multiple_issues(issues)
        daily_issues = self.aggregator.get_daily_issues("2024-01-01")
        self.assertEqual(len(daily_issues["2024-01-01"]), 2)
