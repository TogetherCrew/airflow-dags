import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from hivemind_etl_helpers.src.db.github.load import PrepareDeletion
from llama_index.core import Document


class TestPrepareDeletion(unittest.TestCase):
    def setUp(self):
        self.prepare_deletion = PrepareDeletion("test_community")

    @patch("hivemind_etl_helpers.src.utils.check_documents.fetch_files_date_field")
    def test_prepare(self, mock_fetch_files_date_field):
        mock_fetch_files_date_field.return_value = {
            "1": datetime(2024, 2, 26, tzinfo=timezone.utc),
            "2": datetime(2024, 2, 25, tzinfo=timezone.utc),
            "3": datetime(2024, 2, 24, tzinfo=timezone.utc),
        }

        pr_documents = [
            Document(
                id=1,
                metadata={
                    "id": 1,
                    "merged_at": "2024-02-26",
                    "closed_at": "2024-02-27",
                },
            )
        ]

        issue_documents = [
            Document(id=2, metadata={"id": 2, "updated_at": "2024-02-25 00:01:00"})
        ]
        comment_documents = [
            Document(id=3, metadata={"id": 3, "updated_at": "2024-02-24 01:00:00"})
        ]

        documents_to_save, deletion_query = self.prepare_deletion.prepare(
            pr_documents, issue_documents, comment_documents
        )

        # Check if documents to save are as expected
        self.assertEqual(len(documents_to_save), 3)

        # Check if deletion query is as expected
        expected_query = """
            DELETE FROM data_github
            WHERE (metadata_->>'id')::text IN ('2', '3', '1');
        """
        self.assertEqual(deletion_query.strip(), expected_query.strip())

    @patch("hivemind_etl_helpers.src.utils.check_documents.fetch_files_date_field")
    def test_delete_issue_and_comment_docs(self, mock_fetch_files_date_field):
        mock_fetch_files_date_field.return_value = {
            "1": datetime(2024, 2, 26, tzinfo=timezone.utc),
            "2": datetime(2024, 2, 25, tzinfo=timezone.utc),
        }

        issue_documents = [
            Document(id=1, metadata={"id": 1, "updated_at": "2024-02-27"})
        ]
        comment_documents = [
            Document(id=2, metadata={"id": 2, "updated_at": "2024-02-26"})
        ]

        (
            docs_to_save,
            doc_file_ids_to_delete,
        ) = self.prepare_deletion._delete_issue_and_comment_docs(
            issue_documents, comment_documents
        )

        # Check if documents to save are as expected
        self.assertEqual(len(docs_to_save), 2)
        # Check if document IDs to delete are as expected
        self.assertEqual(len(doc_file_ids_to_delete), 2)

    @patch("hivemind_etl_helpers.src.utils.check_documents.fetch_files_date_field")
    def test_delete_pr_document(self, mock_fetch_files_date_field):
        mock_fetch_files_date_field.return_value = {
            "1": datetime(2024, 2, 26, tzinfo=timezone.utc)
        }

        pr_docs = [
            Document(
                id=1,
                metadata={
                    "id": 1,
                    "merged_at": "2024-02-26 01:00:00",
                    "closed_at": "2024-02-26 01:00:00",
                },
            )
        ]

        (
            documents_to_save,
            doc_file_ids_to_delete,
        ) = self.prepare_deletion._delete_pr_document(pr_docs)

        # Check if documents to save are as expected
        self.assertEqual(len(documents_to_save), 1)
        # Check if document IDs to delete are as expected
        self.assertEqual(len(doc_file_ids_to_delete), 1)

    @patch("hivemind_etl_helpers.src.utils.check_documents.fetch_files_date_field")
    def test_create_deletion_query(self, mock_fetch_files_date_field):
        mock_fetch_files_date_field.return_value = {
            "1": datetime(2024, 2, 26, tzinfo=timezone.utc),
            "2": datetime(2024, 2, 25, tzinfo=timezone.utc),
            "3": datetime(2024, 2, 24, tzinfo=timezone.utc),
        }
        doc_ids_to_delete = ["1", "2", "3"]

        deletion_query = self.prepare_deletion._create_deletion_query(doc_ids_to_delete)

        # Check if deletion query is as expected
        expected_query = """
            DELETE FROM data_github
            WHERE (metadata_->>'id')::text IN ('1', '2', '3');
        """
        self.assertEqual(deletion_query.strip(), expected_query.strip())

    @patch("hivemind_etl_helpers.src.utils.check_documents.fetch_files_date_field")
    def test_check_documents_one_doc_one_updated(self, mock_fetch_files_date_field):
        mock_fetch_files_date_field.return_value = {
            "1": datetime(2024, 2, 26, tzinfo=timezone.utc)
        }

        documents = [
            Document(id=1, metadata={"id": 1, "updated_at": "2024-02-26 01:00:00"})
        ]
        (
            docs_to_save,
            doc_file_ids_to_delete,
        ) = self.prepare_deletion._delete_issue_and_comment_docs(documents, documents)

        # Check if documents to save are as expected
        self.assertEqual(len(docs_to_save), 1)
        # Check if document IDs to delete are as expected
        self.assertEqual(len(doc_file_ids_to_delete), 1)

    @patch("hivemind_etl_helpers.src.utils.check_documents.fetch_files_date_field")
    def test_check_documents_multiple_doc_multiple_updated(
        self, mock_fetch_files_date_field
    ):
        mock_fetch_files_date_field.return_value = {
            "1": datetime(2024, 2, 26, tzinfo=timezone.utc),
            "2": datetime(2024, 2, 27, tzinfo=timezone.utc),
            "3": datetime(2024, 2, 28, tzinfo=timezone.utc),
        }

        documents = [
            Document(id=1, metadata={"id": 1, "updated_at": "2024-02-26 01:00:00"}),
            Document(id=2, metadata={"id": 2, "updated_at": "2024-02-27 01:00:00"}),
            Document(id=3, metadata={"id": 3, "updated_at": "2024-02-28 00:00:00"}),
        ]
        (
            docs_to_save,
            doc_file_ids_to_delete,
        ) = self.prepare_deletion._delete_issue_and_comment_docs(documents, documents)

        # Check if documents to save are as expected
        self.assertEqual(len(docs_to_save), 2)
        # Check if document IDs to delete are as expected
        self.assertEqual(len(doc_file_ids_to_delete), 2)

    def test_get_unique_docs(self):
        documents_list1 = [
            Document(id=1, metadata={"id": 1, "updated_at": "2024-02-26 01:00:00"}),
            Document(id=2, metadata={"id": 2, "updated_at": "2024-02-27 01:00:00"}),
            Document(id=3, metadata={"id": 3, "updated_at": "2024-02-28 00:00:00"}),
        ]
        documents_list2 = [
            Document(id=1, metadata={"id": 1, "updated_at": "2024-02-26 01:00:00"}),
            Document(id=2, metadata={"id": 2, "updated_at": "2024-02-27 01:00:00"}),
        ]
        docs = self.prepare_deletion._get_unique_docs(
            documents_list1, documents_list2, identifier="id"
        )
        self.assertEqual(len(docs), 3)
        self.assertEqual(
            docs[0].metadata, {"id": 1, "updated_at": "2024-02-26 01:00:00"}
        )
        self.assertEqual(
            docs[1].metadata, {"id": 2, "updated_at": "2024-02-27 01:00:00"}
        )
        self.assertEqual(
            docs[2].metadata, {"id": 3, "updated_at": "2024-02-28 00:00:00"}
        )
