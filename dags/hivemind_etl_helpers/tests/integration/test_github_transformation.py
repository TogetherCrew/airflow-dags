from datetime import datetime
from unittest import TestCase

from hivemind_etl_helpers.src.db.github.transform import GitHubTransformation
from hivemind_etl_helpers.src.db.github.schema import (
    GitHubComment,
    GitHubCommit,
    GitHubIssue,
    GitHubPullRequest,
)
from llama_index.core import Document


class TestGitHubTransformation(TestCase):
    def setUp(self) -> None:
        self.github_transfomation = GitHubTransformation()

    def test_comment_transform(self):
        input_data = [
            GitHubComment(
                author_name="author #1",
                id=1,
                text="sample message",
                repository_name="SampleRepo",
                created_at=datetime(2023, 11, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                updated_at=datetime(2023, 11, 1, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                latest_saved_at=datetime(2023, 12, 1, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                url="https://github.com/repo/comment/1",
                related_title="Fix item 1",
                related_node="PullRequest",
                reactions={
                    "hooray": 0,
                    "eyes": 0,
                    "heart": 0,
                    "laugh": 0,
                    "confused": 0,
                    "rocket": 0,
                    "plus1": 0,
                    "minus1": 0,
                    "total_count": 0,
                },
            )
        ]
        documents = self.github_transfomation.transform_comments(input_data)
        self.assertEqual(len(documents), 1)
        self.assertIsInstance(documents, list)
        self.assertIsInstance(documents[0], Document)
        self.assertEqual(documents[0].text, "sample message")

        self.assertEqual(
            documents[0].metadata,
            {
                "author_name": "author #1",
                "id": 1,
                "repository_name": "SampleRepo",
                "url": "https://github.com/repo/comment/1",
                "created_at": "2023-11-01 00:00:00",
                "updated_at": "2023-11-01 01:00:00",
                "related_title": "Fix item 1",
                "related_node": "PullRequest",
                "latest_saved_at": "2023-12-01 01:00:00",
                "reactions": {
                    "hooray": 0,
                    "eyes": 0,
                    "heart": 0,
                    "laugh": 0,
                    "confused": 0,
                    "rocket": 0,
                    "plus1": 0,
                    "minus1": 0,
                    "total_count": 0,
                },
                "type": "comment",
            },
        )

    def test_commit_transformation(self):
        input_data = [
            GitHubCommit(
                author_name="author #1",
                message="sample message",
                sha="sha#1000000",
                created_at=datetime(2023, 11, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                latest_saved_at=datetime(2023, 12, 1, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                html_url="https://github.com/repo/commit/1",
                api_url="https://api.github.com/repo/commit/1",
                repository_id=123,
                repository_name="SampleRepo",
                verification="valid",
            )
        ]
        documents = self.github_transfomation.transform_commits(input_data)
        self.assertEqual(len(documents), 1)
        self.assertIsInstance(documents, list)
        self.assertIsInstance(documents[0], Document)
        self.assertEqual(documents[0].text, "sample message")

        self.assertEqual(
            documents[0].metadata,
            {
                "author_name": "author #1",
                "sha": "sha#1000000",
                "created_at": "2023-11-01 00:00:00",
                "latest_saved_at": "2023-12-01 01:00:00",
                "html_url": "https://github.com/repo/commit/1",
                "api_url": "https://api.github.com/repo/commit/1",
                "repository_id": 123,
                "repository_name": "SampleRepo",
                "verification": "valid",
                "type": "commit",
            },
        )

    def test_issue_transformation(self):
        input_data = [
            GitHubIssue(
                id=1,
                author_name="author #1",
                title="sample title",
                text="sample text",
                state="open",
                state_reason=None,
                created_at=datetime(2023, 11, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                updated_at=datetime(2023, 11, 1, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                latest_saved_at=datetime(2023, 12, 1, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                url="https://github.com/repo/issue/1",
                repository_id=123,
                repository_name="SampleRepo",
            )
        ]
        documents, issue_comment_docs = self.github_transfomation.transform_issues(
            input_data
        )
        self.assertEqual(len(documents), 1)
        self.assertIsInstance(documents, list)
        self.assertIsInstance(documents[0], Document)
        self.assertEqual(documents[0].text, "sample title")

        self.assertEqual(
            documents[0].metadata,
            {
                "id": 1,
                "author_name": "author #1",
                "text": "sample text",
                "state": "open",
                "state_reason": None,
                "created_at": "2023-11-01 00:00:00",
                "updated_at": "2023-11-01 01:00:00",
                "latest_saved_at": "2023-12-01 01:00:00",
                "url": "https://github.com/repo/issue/1",
                "repository_id": 123,
                "repository_name": "SampleRepo",
                "type": "issue",
            },
        )
        self.assertEqual(len(issue_comment_docs), 1)
        self.assertIsInstance(issue_comment_docs, list)
        self.assertIsInstance(issue_comment_docs[0], Document)
        self.assertEqual(issue_comment_docs[0].text, "sample text")

        self.assertEqual(
            issue_comment_docs[0].metadata,
            {
                "author_name": "author #1",
                "id": 111,
                "repository_name": "SampleRepo",
                "url": "https://github.com/repo/issue/1",
                "created_at": "2023-11-01 00:00:00",
                "updated_at": "2023-11-01 01:00:00",
                "related_node": "Issue",
                "related_title": "sample title",
                "latest_saved_at": "2023-12-01 01:00:00",
                "reactions": {},
                "type": "comment",
            },
        )

    def test_pull_request_transformation(self):
        input_data = [
            GitHubPullRequest(
                author_name="author #1",
                repository_id=123,
                repository_name="SampleRepo",
                issue_url="https://api.github.com/repo/issue/1",
                created_at=datetime(2023, 11, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                title="sample title",
                id=1,
                closed_at=None,
                merged_at=None,
                state="open",
                url="https://github.com/repo/pull/1",
                latest_saved_at=datetime(2023, 12, 1, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
        ]
        documents = self.github_transfomation.transform_pull_requests(input_data)
        self.assertEqual(len(documents), 1)
        self.assertIsInstance(documents, list)
        self.assertIsInstance(documents[0], Document)
        self.assertEqual(documents[0].text, "sample title")

        self.assertEqual(
            documents[0].metadata,
            {
                "author_name": "author #1",
                "repository_id": 123,
                "repository_name": "SampleRepo",
                "issue_url": "https://api.github.com/repo/issue/1",
                "created_at": "2023-11-01 00:00:00",
                "latest_saved_at": "2023-12-01 01:00:00",
                "id": 1,
                "closed_at": None,
                "merged_at": None,
                "state": "open",
                "url": "https://github.com/repo/pull/1",
                "type": "pull_request",
            },
        )
