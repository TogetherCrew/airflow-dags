from datetime import datetime
from unittest import TestCase

from hivemind_etl_helpers.src.db.github.schema import GitHubIssue
from hivemind_etl_helpers.src.db.github.transform.issues import transform_issues
from llama_index.core import Document


class TestGithubTransformIssues(TestCase):
    def test_github_no_document(self):
        documents, issue_comment_docs = transform_issues(data=[])
        self.assertEqual(documents, [])
        self.assertEqual(issue_comment_docs, [])

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
        documents, issue_comment_docs = transform_issues(input_data)
        self.assertEqual(len(documents), 1)
        self.assertIsInstance(documents, list)
        self.assertIsInstance(documents[0], Document)
        self.assertEqual(documents[0].id_, "1")
        self.assertEqual(documents[0].text, "sample title")

        self.assertEqual(
            documents[0].metadata,
            {
                "id": 1,
                "author_name": "author #1",
                "text": "sample text",
                "state": "open",
                "state_reason": None,
                "created_at": 1698796800.0,
                "updated_at": 1698800400.0,
                "latest_saved_at": 1701392400.0,
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
                "created_at": 1698796800.0,
                "updated_at": 1698800400.0,
                "related_node": "Issue",
                "related_title": "sample title",
                "latest_saved_at": 1701392400.0,
                "reactions": {},
                "type": "comment",
            },
        )
        self.assertEqual(issue_comment_docs[0].id_, "111")

    def test_multiple_documents(self):
        input_data = [
            GitHubIssue(
                id=1234567890,
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
                id=2234567891,
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
                id=3234567892,
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

        documents, issue_comment_docs = transform_issues(input_data)
        self.assertEqual(len(documents), 3)
        self.assertIsInstance(documents, list)
        for doc in documents:
            self.assertIsInstance(doc, Document)

        self.assertEqual(documents[0].id_, "1234567890")
        self.assertEqual(documents[1].id_, "2234567891")
        self.assertEqual(documents[2].id_, "3234567892")

        self.assertEqual(
            documents[0].metadata,
            {
                "id": 1234567890,
                "author_name": "author #1",
                "text": "sample text #1",
                "state": "open",
                "state_reason": None,
                "created_at": 1698796800.0,
                "updated_at": 1698800400.0,
                "latest_saved_at": 1701392400.0,
                "url": "https://github.com/repo/issue/1",
                "repository_id": 123,
                "repository_name": "SampleRepo",
                "type": "issue",
            },
        )

        self.assertEqual(
            documents[1].metadata,
            {
                "id": 2234567891,
                "author_name": "author #2",
                "text": "sample text #2",
                "state": "open",
                "state_reason": None,
                "created_at": 1698883200.0,
                "updated_at": 1698886800.0,
                "latest_saved_at": 1701478800.0,
                "url": "https://github.com/repo/issue/2",
                "repository_id": 123,
                "repository_name": "SampleRepo",
                "type": "issue",
            },
        )

        self.assertEqual(
            documents[2].metadata,
            {
                "id": 3234567892,
                "author_name": "author #3",
                "text": "sample text #3",
                "state": "open",
                "state_reason": None,
                "created_at": 1698969600.0,
                "updated_at": 1698973200.0,
                "latest_saved_at": 1701565200.0,
                "url": "https://github.com/repo/issue/3",
                "repository_id": 125,
                "repository_name": "SampleRepo#5",
                "type": "issue",
            },
        )

        self.assertEqual(len(issue_comment_docs), 3)
        self.assertIsInstance(issue_comment_docs, list)
        for doc in issue_comment_docs:
            self.assertIsInstance(doc, Document)

        self.assertEqual(issue_comment_docs[0].text, "sample text #1")
        self.assertEqual(issue_comment_docs[1].text, "sample text #2")
        self.assertEqual(issue_comment_docs[2].text, "sample text #3")

        self.assertEqual(issue_comment_docs[0].id_, "1114567890")
        self.assertEqual(issue_comment_docs[1].id_, "1114567891")
        self.assertEqual(issue_comment_docs[2].id_, "1114567892")

        self.assertEqual(
            issue_comment_docs[0].metadata,
            {
                "author_name": "author #1",
                "id": 1114567890,
                "repository_name": "SampleRepo",
                "url": "https://github.com/repo/issue/1",
                "created_at": 1698796800.0,
                "updated_at": 1698800400.0,
                "related_node": "Issue",
                "related_title": "sample title #1",
                "latest_saved_at": 1701392400.0,
                "reactions": {},
                "type": "comment",
            },
        )

        self.assertEqual(
            issue_comment_docs[1].metadata,
            {
                "author_name": "author #2",
                "id": 1114567891,
                "repository_name": "SampleRepo",
                "url": "https://github.com/repo/issue/2",
                "created_at": 1698883200.0,
                "updated_at": 1698886800.0,
                "related_node": "Issue",
                "related_title": "sample title #2",
                "latest_saved_at": 1701478800.0,
                "reactions": {},
                "type": "comment",
            },
        )

        self.assertEqual(
            issue_comment_docs[2].metadata,
            {
                "author_name": "author #3",
                "id": 1114567892,
                "repository_name": "SampleRepo#5",
                "url": "https://github.com/repo/issue/3",
                "created_at": 1698969600.0,
                "updated_at": 1698973200.0,
                "related_node": "Issue",
                "related_title": "sample title #3",
                "latest_saved_at": 1701565200.0,
                "reactions": {},
                "type": "comment",
            },
        )
