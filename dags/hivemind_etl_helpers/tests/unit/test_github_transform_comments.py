from datetime import datetime
from unittest import TestCase

from hivemind_etl_helpers.src.db.github.schema import GitHubComment
from hivemind_etl_helpers.src.db.github.transform.comments import transform_comments
from llama_index.core import Document


class TestGithubTransformcomComments(TestCase):
    def test_github_no_document(self):
        documents = transform_comments(data=[])
        self.assertEqual(documents, [])

    def test_github_single_document(self):
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
        documents = transform_comments(input_data)
        self.assertEqual(len(documents), 1)
        self.assertIsInstance(documents, list)
        self.assertIsInstance(documents[0], Document)
        self.assertEqual(documents[0].text, "sample message")
        self.assertEqual(documents[0].id_, "1")

        self.assertEqual(
            documents[0].metadata,
            {
                "author_name": "author #1",
                "id": 1,
                "repository_name": "SampleRepo",
                "url": "https://github.com/repo/comment/1",
                "created_at": 1698796800.0,
                "updated_at": 1698800400.0,
                "related_title": "Fix item 1",
                "related_node": "PullRequest",
                "latest_saved_at": 1701392400.0,
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

    def test_multiple_documents(self):
        input_data = [
            GitHubComment(
                author_name="author #1",
                id=1,
                text="sample message #1",
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
            ),
            GitHubComment(
                author_name="author #2",
                text="sample message #2",
                id=2,
                repository_name="SampleRepo",
                created_at=datetime(2023, 11, 2).strftime("%Y-%m-%dT%H:%M:%SZ"),
                updated_at=datetime(2023, 11, 2, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                latest_saved_at=datetime(2023, 12, 2, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                url="https://github.com/repo/comment/2",
                related_title="Problem on item 2",
                related_node="Issue",
                reactions={
                    "hooray": 0,
                    "eyes": 0,
                    "heart": 0,
                    "laugh": 0,
                    "confused": 2,
                    "rocket": 0,
                    "plus1": 1,
                    "minus1": 0,
                    "total_count": 3,
                },
            ),
            GitHubComment(
                author_name="author #3",
                id=3,
                text="sample message #3",
                repository_name="SampleRepo#6",
                created_at=datetime(2023, 11, 3).strftime("%Y-%m-%dT%H:%M:%SZ"),
                updated_at=datetime(2023, 11, 3, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                latest_saved_at=datetime(2023, 12, 3, 1).strftime("%Y-%m-%dT%H:%M:%SZ"),
                url="https://github.com/repo/comment/3",
                related_title="Fix item 2",
                related_node="PullRequest",
                reactions={
                    "hooray": 1,
                    "eyes": 1,
                    "heart": 0,
                    "laugh": 0,
                    "confused": 0,
                    "rocket": 0,
                    "plus1": 0,
                    "minus1": 0,
                    "total_count": 2,
                },
            ),
        ]

        documents = transform_comments(input_data)
        self.assertEqual(len(documents), 3)
        self.assertIsInstance(documents, list)
        for doc in documents:
            self.assertIsInstance(doc, Document)

        self.assertEqual(documents[0].id_, "1")
        self.assertEqual(documents[1].id_, "2")
        self.assertEqual(documents[2].id_, "3")

        self.assertEqual(
            documents[0].metadata,
            {
                "author_name": "author #1",
                "id": 1,
                "repository_name": "SampleRepo",
                "url": "https://github.com/repo/comment/1",
                "created_at": 1698796800.0,
                "updated_at": 1698800400.0,
                "related_title": "Fix item 1",
                "related_node": "PullRequest",
                "latest_saved_at": 1701392400.0,
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

        self.assertEqual(
            documents[1].metadata,
            {
                "author_name": "author #2",
                "id": 2,
                "repository_name": "SampleRepo",
                "url": "https://github.com/repo/comment/2",
                "created_at": 1698883200.0,
                "updated_at": 1698886800.0,
                "related_title": "Problem on item 2",
                "related_node": "Issue",
                "latest_saved_at": 1701478800.0,
                "reactions": {
                    "hooray": 0,
                    "eyes": 0,
                    "heart": 0,
                    "laugh": 0,
                    "confused": 2,
                    "rocket": 0,
                    "plus1": 1,
                    "minus1": 0,
                    "total_count": 3,
                },
                "type": "comment",
            },
        )

        self.assertEqual(
            documents[2].metadata,
            {
                "author_name": "author #3",
                "id": 3,
                "repository_name": "SampleRepo#6",
                "url": "https://github.com/repo/comment/3",
                "created_at": 1698969600.0,
                "updated_at": 1698973200.0,
                "related_title": "Fix item 2",
                "related_node": "PullRequest",
                "latest_saved_at": 1701565200.0,
                "reactions": {
                    "hooray": 1,
                    "eyes": 1,
                    "heart": 0,
                    "laugh": 0,
                    "confused": 0,
                    "rocket": 0,
                    "plus1": 0,
                    "minus1": 0,
                    "total_count": 2,
                },
                "type": "comment",
            },
        )
