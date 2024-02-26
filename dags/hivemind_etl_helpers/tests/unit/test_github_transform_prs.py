from unittest import TestCase
from datetime import datetime

from llama_index import Document
from hivemind_etl_helpers.src.db.github.transform.pull_requests import transform_prs
from hivemind_etl_helpers.src.db.github.utils.schema import GitHubPullRequest


class TestGithubTransformPRs(TestCase):
    def test_github_no_document(self):
        documents = transform_prs(data=[])
        self.assertEqual(documents, [])

    def test_github_single_document(self):
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
        documents = transform_prs(input_data)
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

    def test_multiple_documents(self):
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
            ),
            GitHubPullRequest(
                author_name="author #2",
                repository_id=123,
                repository_name="SampleRepo",
                issue_url="https://api.github.com/repo/issue/2",
                created_at=datetime(2023, 11, 2).strftime("%Y-%m-%dT%H:%M:%SZ"),
                title="sample title #2",
                id=2,
                closed_at=None,
                merged_at=None,
                state="open",
                url="https://github.com/repo/pull/2",
                latest_saved_at=datetime(2023, 12, 2, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
            ),
            GitHubPullRequest(
                author_name="author #3",
                repository_id=125,
                repository_name="SampleRepo#5",
                issue_url="https://api.github.com/repo/issue/3",
                created_at=datetime(2023, 11, 3).strftime("%Y-%m-%dT%H:%M:%SZ"),
                title="sample title #3",
                id=3,
                closed_at=None,
                merged_at=None,
                state="open",
                url="https://github.com/repo/pull/3",
                latest_saved_at=datetime(2023, 12, 3, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
            ),
        ]

        documents = transform_prs(input_data)
        self.assertEqual(len(documents), 3)
        self.assertIsInstance(documents, list)
        for doc in documents:
            self.assertIsInstance(doc, Document)

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

        self.assertEqual(
            documents[1].metadata,
            {
                "author_name": "author #2",
                "repository_id": 123,
                "repository_name": "SampleRepo",
                "issue_url": "https://api.github.com/repo/issue/2",
                "created_at": "2023-11-02 00:00:00",
                "latest_saved_at": "2023-12-02 01:00:00",
                "id": 2,
                "closed_at": None,
                "merged_at": None,
                "state": "open",
                "url": "https://github.com/repo/pull/2",
                "type": "pull_request",
            },
        )

        self.assertEqual(
            documents[2].metadata,
            {
                "author_name": "author #3",
                "repository_id": 125,
                "repository_name": "SampleRepo#5",
                "issue_url": "https://api.github.com/repo/issue/3",
                "created_at": "2023-11-03 00:00:00",
                "latest_saved_at": "2023-12-03 01:00:00",
                "id": 3,
                "closed_at": None,
                "merged_at": None,
                "state": "open",
                "url": "https://github.com/repo/pull/3",
                "type": "pull_request",
            },
        )
