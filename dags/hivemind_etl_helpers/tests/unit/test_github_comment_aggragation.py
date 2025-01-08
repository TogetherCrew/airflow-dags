import unittest

from hivemind_etl_helpers.src.db.github.aggregator import CommentAggregator
from hivemind_etl_helpers.src.db.github.schema import GitHubComment


class TestGitHubCommentAggregator(unittest.TestCase):
    def setUp(self):
        self.aggregator = CommentAggregator()

        # Create sample comment data
        self.sample_reactions = {"like": 2, "heart": 1}
        self.comment1 = GitHubComment(
            author_name="user1",
            id=1,
            repository_name="repo1",
            url="http://example.com/1",
            created_at="2024-01-01 10:00:00",
            updated_at="2024-01-01 11:00:00",
            related_title="Issue 1",
            related_node="node1",
            text="Test comment 1",
            latest_saved_at="2024-01-01 12:00:00",
            reactions=self.sample_reactions,
        )

        self.comment2 = GitHubComment(
            author_name="user2",
            id=2,
            repository_name="repo1",
            url="http://example.com/2",
            created_at="2024-01-01 13:00:00",
            updated_at="2024-01-01 14:00:00",
            related_title="Issue 2",
            related_node="node2",
            text="Test comment 2",
            latest_saved_at="2024-01-01 15:00:00",
            reactions={"like": 1},
        )
        self.comment3 = GitHubComment(
            author_name="user3",
            id=3,
            repository_name="repo1",
            url="http://example.com/3",
            created_at="2024-01-02 13:00:00",
            updated_at="2024-01-02 14:00:00",
            related_title="Issue 3",
            related_node="node3",
            text="Test comment 3",
            latest_saved_at="2024-01-03 15:00:00",
            reactions={"like": 1},
        )

    def test_add_single_comment(self):
        self.aggregator.add_comment(self.comment1)
        daily_comments = self.aggregator.get_daily_comments("2024-01-01")
        self.assertEqual(len(daily_comments["2024-01-01"]), 1)
        self.assertEqual(daily_comments["2024-01-01"][0].id, 1)

    def test_add_multiple_comments(self):
        comments = [self.comment1, self.comment2, self.comment3]
        self.aggregator.add_multiple_comments(comments)
        daily_comments = self.aggregator.get_daily_comments("2024-01-01")
        self.assertEqual(len(daily_comments["2024-01-01"]), 2)

    def test_add_multiple_comments_all_comments(self):
        comments = [self.comment1, self.comment2, self.comment3]
        self.aggregator.add_multiple_comments(comments)
        daily_comments = self.aggregator.get_daily_comments(date=None)
        self.assertEqual(len(daily_comments["2024-01-01"]), 2)
        self.assertEqual(len(daily_comments["2024-01-02"]), 1)
