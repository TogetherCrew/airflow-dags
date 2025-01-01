import unittest

from hivemind_etl_helpers.src.db.github.schema import GitHubPullRequest
from hivemind_etl_helpers.src.db.github.aggregator import PullRequestAggregator


class TestPullRequestAggregator(unittest.TestCase):
    def setUp(self):
        self.aggregator = PullRequestAggregator()

        self.pr1 = GitHubPullRequest(
            author_name="user1",
            repository_id=1,
            repository_name="repo1",
            issue_url="http://example.com/issues/1",
            created_at="2024-01-01 10:00:00",
            title="Test PR 1",
            id=1,
            closed_at=None,
            merged_at=None,
            state="open",
            url="http://example.com/pr/1",
            latest_saved_at="2024-01-01 12:00:00",
        )

        self.pr2 = GitHubPullRequest(
            author_name="user2",
            repository_id=1,
            repository_name="repo1",
            issue_url="http://example.com/issues/2",
            created_at="2024-01-01 13:00:00",
            title="Test PR 2",
            id=2,
            closed_at="2024-01-01 14:00:00",
            merged_at="2024-01-01 14:00:00",
            state="merged",
            url="http://example.com/pr/2",
            latest_saved_at="2024-01-01 15:00:00",
        )

    def test_add_single_pr(self):
        self.aggregator.add_pr(self.pr1)
        daily_prs = self.aggregator.get_daily_prs("2024-01-01")
        self.assertEqual(len(daily_prs["2024-01-01"]), 1)
        self.assertEqual(daily_prs["2024-01-01"][0]["id"], 1)

    def test_add_multiple_prs(self):
        prs = [self.pr1, self.pr2]
        self.aggregator.add_multiple_prs(prs)
        daily_prs = self.aggregator.get_daily_prs("2024-01-01")
        self.assertEqual(len(daily_prs["2024-01-01"]), 2)
