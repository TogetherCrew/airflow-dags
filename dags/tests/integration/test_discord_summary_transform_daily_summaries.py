import unittest

from llama_index import Document

from hivemind_etl_helpers.src.db.discord.summary.summary_utils import (
    transform_daily_summary_to_document,
)


class TestTransformDailySummaryToDocument(unittest.TestCase):
    def test_transform_daily_summary_to_document_empty_data(self):
        daily_summary = {}

        result_documents = transform_daily_summary_to_document(daily_summary)
        self.assertEqual(result_documents, [])

    def test_transform_daily_summary_to_document(self):
        # Input daily summary dictionary
        daily_summary = {
            "2023-01-01": "Summary 1",
            "2023-01-02": "Summary 2",
            "2023-01-03": "Summary 3",
        }

        # Expected output documents
        expected_documents = [
            Document(text="Summary 1", metadata={"date": "2023-01-01"}),
            Document(text="Summary 2", metadata={"date": "2023-01-02"}),
            Document(text="Summary 3", metadata={"date": "2023-01-03"}),
        ]

        # Transform and get result
        result_documents = transform_daily_summary_to_document(daily_summary)

        # Assertions
        self.assertEqual(len(result_documents), len(expected_documents))

        for result_doc, expected_doc in zip(result_documents, expected_documents):
            self.assertEqual(result_doc.text, expected_doc.text)
            self.assertEqual(result_doc.metadata, expected_doc.metadata)
