from datetime import datetime, timezone
from unittest import TestCase
import pytest

from hivemind_etl_helpers.src.utils.check_documents import process_doc_to_id_date
from llama_index.core import Document


@pytest.mark.skip(reason="GDrive ETL is not finished!")
class TestDocumentGdriveProcessing(TestCase):
    def test_empty_documents(self):
        result = process_doc_to_id_date(
            [], identifier="file id", date_field="modified at"
        )
        self.assertEqual(result, {})

    def test_single_document(self):
        documents = [
            Document(
                metadata={"file id": "123", "modified at": "2023-11-15T06:02:09.828Z"}
            )
        ]
        result = process_doc_to_id_date(
            documents, identifier="file id", date_field="modified at"
        )
        expected = {"123": datetime(2023, 11, 15, 6, 2, 9, 828000, tzinfo=timezone.utc)}
        self.assertEqual(result, expected)

    def test_multiple_documents(self):
        documents = [
            Document(
                metadata={"file id": "123", "modified at": "2023-11-15T06:02:09.828Z"}
            ),
            Document(
                metadata={"file id": "456", "modified at": "2022-10-20T12:30:45.500Z"}
            ),
            Document(
                metadata={"file id": "789", "modified at": "2021-09-10T08:15:30.200Z"}
            ),
        ]
        result = process_doc_to_id_date(
            documents, identifier="file id", date_field="modified at"
        )
        expected = {
            "123": datetime(2023, 11, 15, 6, 2, 9, 828000, tzinfo=timezone.utc),
            "456": datetime(2022, 10, 20, 12, 30, 45, 500000, tzinfo=timezone.utc),
            "789": datetime(2021, 9, 10, 8, 15, 30, 200000, tzinfo=timezone.utc),
        }
        self.assertEqual(result, expected)

    def test_missing_metadata_fields(self):
        documents = [Document(metadata={"file id": "123"})]
        with self.assertRaises(KeyError):
            process_doc_to_id_date(
                documents, identifier="file id", date_field="modified at"
            )
