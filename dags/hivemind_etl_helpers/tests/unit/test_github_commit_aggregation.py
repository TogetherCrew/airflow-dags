import unittest
from dateutil.parser import parse

from hivemind_etl_helpers.src.db.github.aggregator import CommitAggregator
from hivemind_etl_helpers.src.db.github.schema import GitHubCommit


class TestCommitAggregator(unittest.TestCase):
    def setUp(self):
        self.aggregator = CommitAggregator()

        self.commit1 = GitHubCommit(
            author_name="user1",
            committer_name="user1",
            message="Test commit 1",
            api_url="http://api.example.com/1",
            url="http://example.com/1",
            repository_id=1,
            repository_name="repo1",
            sha="abc123",
            latest_saved_at="2024-01-01 12:00:00",
            created_at="2024-01-01 10:00:00",
            verification="verified",
            related_pr_title="PR 1",
        )

        self.commit2 = GitHubCommit(
            author_name="user2",
            committer_name="user2",
            message="Test commit 2",
            api_url="http://api.example.com/2",
            url="http://example.com/2",
            repository_id=2,
            repository_name="repo2",
            sha="def456",
            latest_saved_at="2024-01-01 15:00:00",
            created_at="2024-01-01 13:00:00",
            verification="verified",
            related_pr_title="PR 2",
        )

    def test_add_single_commit(self):
        self.aggregator.add_commit(self.commit1)
        date = parse("2024-01-01").timestamp()

        daily_commits = self.aggregator.get_daily_commits(date)
        self.assertEqual(len(daily_commits[date]), 1)
        self.assertEqual(daily_commits[date][0].sha, "abc123")

    def test_add_multiple_commits(self):
        commits = [self.commit1, self.commit2]
        self.aggregator.add_multiple_commits(commits)
        date = parse("2024-01-01").timestamp()

        daily_commits = self.aggregator.get_daily_commits(date)
        self.assertEqual(len(daily_commits[date]), 2)
