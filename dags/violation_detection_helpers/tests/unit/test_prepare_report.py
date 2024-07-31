from unittest import TestCase
from unittest.mock import patch
from bson import ObjectId
from datetime import datetime
from violation_detection_helpers.utils import PrepareReport


class TestPrepareReport(TestCase):
    def setUp(self):
        self.preparer = PrepareReport()

    def test_empty_input(self):
        result = self.preparer.prepare([])
        self.assertIsNone(result, "Expected None for empty input")

    def test_valid_input(self):
        transformed_documents = [
            {
                "_id": ObjectId("669f40ac65aef1417bdda9a9"),
                "metadata": {
                    "thread_id": None,
                    "channel_id": "915944557605163008",
                    "bot_activity": False,
                    "vdLabel": "Identifying",
                    "link": "https://sample_link.com",
                },
                "actions": [{"name": "message", "type": "emitter"}],
                "author_id": "729635673383895111",
                "date": datetime(2023, 6, 30),
                "interactions": [],
                "source_id": "1124399166827794582",
            }
        ]
        result = self.preparer.prepare(transformed_documents)
        expected_report = (
            "Here's a list of messages with detected violation\n\n"
            "Document link: https://sample_link.com | Label: Identifying\n"
        )
        self.assertEqual(result, expected_report, "Expected a valid report string")

    @patch("logging.error")
    def test_missing_link(self, mock_logging_error):
        transformed_documents = [
            {
                "_id": ObjectId("669f40ac65aef1417bdda9a9"),
                "actions": [{"name": "message", "type": "emitter"}],
                "author_id": "729635673383895111",
                "date": datetime(2023, 6, 30),
                "interactions": [],
                "metadata": {
                    "thread_id": None,
                    "channel_id": "915944557605163008",
                    "bot_activity": False,
                    "vdLabel": "Identifying",
                },
                "source_id": "1124399166827794582",
            }
        ]
        result = self.preparer.prepare(transformed_documents)
        self.assertIn(
            "Document with _id",
            mock_logging_error.call_args[0][0],
            "Expected logging error for missing link",
        )
        self.assertIn(
            "metadata.link",
            mock_logging_error.call_args[0][0],
            "Expected logging error for missing link",
        )
        self.assertIsNone(
            result, "Not expecting a report if no document was having the right fields!"
        )

    @patch("logging.error")
    def test_missing_vdLabel(self, mock_logging_error):
        transformed_documents = [
            {
                "_id": ObjectId("669f40ac65aef1417bdda9a9"),
                "metadata": {
                    "link": "https://sample_link.com",
                    "thread_id": None,
                    "channel_id": "915944557605163008",
                    "bot_activity": False,
                },
                "actions": [{"name": "message", "type": "emitter"}],
                "author_id": "729635673383895111",
                "date": datetime(2023, 6, 30),
                "interactions": [],
            }
        ]
        result = self.preparer.prepare(transformed_documents)
        self.assertIn(
            "Document with _id",
            mock_logging_error.call_args[0][0],
            "Expected logging error for missing vdLabel",
        )
        self.assertIn(
            "metadata.vdLabel",
            mock_logging_error.call_args[0][0],
            "Expected logging error for missing vdLabel",
        )
        self.assertIsNone(
            result, "Not expecting a report if no document was having the right fields!"
        )

    @patch("logging.error")
    def test_missing_both_vdLabel_and_link(self, mock_logging_error):
        transformed_documents = [
            {
                "_id": ObjectId("669f40ac65aef1417bdda9a9"),
                "actions": [{"name": "message", "type": "emitter"}],
                "author_id": "729635673383895111",
                "date": datetime(2023, 6, 30),
                "interactions": [],
                "metadata": {
                    "thread_id": None,
                    "channel_id": "915944557605163008",
                    "bot_activity": False,
                },
                "source_id": "1124399166827794582",
            }
        ]
        result = self.preparer.prepare(transformed_documents)
        self.assertIn(
            "Document with _id",
            mock_logging_error.call_args[0][0],
            "Expected logging error for missing both vdLabel and link",
        )
        self.assertIn(
            "`metadata.vdLabel` and `metadata.link`",
            mock_logging_error.call_args[0][0],
            "Expected logging error for missing both vdLabel and link",
        )
        self.assertIsNone(
            result, "Not expecting a report if no document was having the right fields!"
        )
