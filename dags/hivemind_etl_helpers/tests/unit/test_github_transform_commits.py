from datetime import datetime
from unittest import TestCase

from hivemind_etl_helpers.src.db.github.schema import GitHubCommit
from hivemind_etl_helpers.src.db.github.transform.commits import transform_commits
from llama_index.core import Document


class TestGithubTransformCommits(TestCase):
    def test_github_no_document(self):
        documents = transform_commits(data=[])
        self.assertEqual(documents, [])

    def test_github_single_document(self):
        input_data = [
            GitHubCommit(
                author_name="author #1",
                committer_name="author #2",
                message="sample message",
                sha="sha#1000000",
                created_at=datetime(2023, 11, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                latest_saved_at=datetime(2023, 12, 1, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                url="https://github.com/repo/commit/1",
                api_url="https://api.github.com/repo/commit/1",
                repository_id=123,
                repository_name="SampleRepo",
                verification="valid",
            )
        ]
        documents = transform_commits(input_data)
        self.assertEqual(len(documents), 1)
        self.assertIsInstance(documents, list)
        self.assertIsInstance(documents[0], Document)
        self.assertEqual(documents[0].id_, "sha#1000000")
        self.assertEqual(documents[0].text, "sample message")

        self.assertEqual(
            documents[0].metadata,
            {
                "author_name": "author #1",
                "committer_name": "author #2",
                "sha": "sha#1000000",
                "created_at": "2023-11-01 00:00:00",
                "latest_saved_at": "2023-12-01 01:00:00",
                "url": "https://github.com/repo/commit/1",
                "api_url": "https://api.github.com/repo/commit/1",
                "repository_id": 123,
                "repository_name": "SampleRepo",
                "verification": "valid",
                "type": "commit",
                "related_pr_title": None,
            },
        )

    def test_multiple_documents(self):
        input_data = [
            GitHubCommit(
                author_name="author #1",
                committer_name="author #1",
                message="sample message #1",
                sha="sha#1000000",
                created_at=datetime(2023, 11, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                latest_saved_at=datetime(2023, 12, 1, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                url="https://github.com/repo/commit/1",
                api_url="https://api.github.com/repo/commit/1",
                repository_id=123,
                repository_name="SampleRepo",
                verification="valid",
                related_pr_title="Sample PR",
            ),
            GitHubCommit(
                author_name="author #2",
                committer_name="author #3",
                message="sample message #2",
                sha="sha#1000001",
                created_at=datetime(2023, 11, 2).strftime("%Y-%m-%dT%H:%M:%SZ"),
                latest_saved_at=datetime(2023, 12, 2, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                url="https://github.com/repo/commit/2",
                api_url="https://api.github.com/repo/commit/2",
                repository_id=123,
                repository_name="SampleRepo",
                verification="valid",
            ),
            GitHubCommit(
                author_name="author #3",
                committer_name="author #4",
                message="sample message #3",
                sha="sha#1000002",
                created_at=datetime(2023, 11, 3).strftime("%Y-%m-%dT%H:%M:%SZ"),
                latest_saved_at=datetime(2023, 12, 3, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                url="https://github.com/repo/commit/3",
                api_url="https://api.github.com/repo/commit/3",
                repository_id=126,
                repository_name="SampleRepo#6",
                verification="unsigned",
            ),
        ]

        documents = transform_commits(input_data)
        self.assertEqual(len(documents), 3)
        self.assertIsInstance(documents, list)
        for doc in documents:
            self.assertIsInstance(doc, Document)

        self.assertEqual(documents[0].id_, "sha#1000000")
        self.assertEqual(documents[1].id_, "sha#1000001")
        self.assertEqual(documents[2].id_, "sha#1000002")

        self.assertEqual(
            documents[0].metadata,
            {
                "author_name": "author #1",
                "committer_name": "author #1",
                "sha": "sha#1000000",
                "created_at": "2023-11-01 00:00:00",
                "latest_saved_at": "2023-12-01 01:00:00",
                "url": "https://github.com/repo/commit/1",
                "api_url": "https://api.github.com/repo/commit/1",
                "repository_id": 123,
                "repository_name": "SampleRepo",
                "verification": "valid",
                "type": "commit",
                "related_pr_title": "Sample PR",
            },
        )

        self.assertEqual(
            documents[1].metadata,
            {
                "author_name": "author #2",
                "committer_name": "author #3",
                "sha": "sha#1000001",
                "created_at": "2023-11-02 00:00:00",
                "latest_saved_at": "2023-12-02 01:00:00",
                "url": "https://github.com/repo/commit/2",
                "api_url": "https://api.github.com/repo/commit/2",
                "repository_id": 123,
                "repository_name": "SampleRepo",
                "verification": "valid",
                "type": "commit",
                "related_pr_title": None,
            },
        )

        self.assertEqual(
            documents[2].metadata,
            {
                "author_name": "author #3",
                "committer_name": "author #4",
                "sha": "sha#1000002",
                "created_at": "2023-11-03 00:00:00",
                "latest_saved_at": "2023-12-03 01:00:00",
                "url": "https://github.com/repo/commit/3",
                "api_url": "https://api.github.com/repo/commit/3",
                "repository_id": 126,
                "repository_name": "SampleRepo#6",
                "verification": "unsigned",
                "type": "commit",
                "related_pr_title": None,
            },
        )
