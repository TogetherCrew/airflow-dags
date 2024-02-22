from unittest import TestCase
from datetime import datetime

from llama_index import Document
from hivemind_etl_helpers.src.db.github.transform.issues import transform_issues
from hivemind_etl_helpers.src.db.github.utils.schema import GitHubIssue


class TestGithubTransformIssues(TestCase):
    def test_github_no_document(self):
        documents = transform_issues(data=[])
        self.assertEqual(documents, [])

    def test_github_single_document(self):
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
        documents = transform_issues(input_data)
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
                "created_at": datetime(2023, 11, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "updated_at": datetime(2023, 11, 1, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "latest_saved_at": datetime(2023, 12, 1, 1).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "url": "https://github.com/repo/issue/1",
                "repository_id": 123,
                "repository_name": "SampleRepo",
                "type": "issue",
            },
        )

    def test_multiple_documents(self):
        input_data = [
            GitHubIssue(
                id=1,
                author_name="author #1",
                title="sample title #1",
                text="sample text #1",
                state="open",
                state_reason=None,
                created_at=datetime(2023, 11, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                updated_at=datetime(2023, 11, 1, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                latest_saved_at=datetime(2023, 12, 1, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                url="https://github.com/repo/issue/1",
                repository_id=123,
                repository_name="SampleRepo",
            ),
            GitHubIssue(
                id=2,
                author_name="author #2",
                title="sample title #2",
                text="sample text #2",
                state="open",
                state_reason=None,
                created_at=datetime(2023, 11, 2).strftime("%Y-%m-%dT%H:%M:%SZ"),
                updated_at=datetime(2023, 11, 2, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                latest_saved_at=datetime(2023, 12, 2, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                url="https://github.com/repo/issue/2",
                repository_id=123,
                repository_name="SampleRepo",
            ),
            GitHubIssue(
                id=3,
                author_name="author #3",
                title="sample title #3",
                text="sample text #3",
                state="open",
                state_reason=None,
                created_at=datetime(2023, 11, 3).strftime("%Y-%m-%dT%H:%M:%SZ"),
                updated_at=datetime(2023, 11, 3, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                latest_saved_at=datetime(2023, 12, 3, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                url="https://github.com/repo/issue/3",
                repository_id=125,
                repository_name="SampleRepo#5",
            ),
        ]

        documents = transform_issues(input_data)
        self.assertEqual(len(documents), 3)
        self.assertIsInstance(documents, list)
        for doc in documents:
            self.assertIsInstance(doc, Document)

        self.assertEqual(
            documents[0].metadata,
            {
                "id": 1,
                "author_name": "author #1",
                "text": "sample text #1",
                "state": "open",
                "state_reason": None,
                "created_at": datetime(2023, 11, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "updated_at": datetime(2023, 11, 1, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "latest_saved_at": datetime(2023, 12, 1, 1).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "url": "https://github.com/repo/issue/1",
                "repository_id": 123,
                "repository_name": "SampleRepo",
                "type": "issue",
            },
        )

        self.assertEqual(
            documents[1].metadata,
            {
                "id": 2,
                "author_name": "author #2",
                "text": "sample text #2",
                "state": "open",
                "state_reason": None,
                "created_at": datetime(2023, 11, 2).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "updated_at": datetime(2023, 11, 2, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "latest_saved_at": datetime(2023, 12, 2, 1).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "url": "https://github.com/repo/issue/2",
                "repository_id": 123,
                "repository_name": "SampleRepo",
                "type": "issue",
            },
        )

        self.assertEqual(
            documents[2].metadata,
            {
                "id": 3,
                "author_name": "author #3",
                "text": "sample text #3",
                "state": "open",
                "state_reason": None,
                "created_at": datetime(2023, 11, 3).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "updated_at": datetime(2023, 11, 3, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "latest_saved_at": datetime(2023, 12, 3, 1).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "url": "https://github.com/repo/issue/3",
                "repository_id": 125,
                "repository_name": "SampleRepo#5",
                "type": "issue",
            },
        )
