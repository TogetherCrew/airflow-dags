import unittest
from unittest.mock import Mock

from dags.hivemind_etl_helpers.ingestion_pipeline import CustomIngestionPipeline
from llama_index.core.ingestion import IngestionPipeline
from llama_index.core.schema import Document


class TestIngestionPipeline(unittest.TestCase):
    def test_run_pipeline(self):
        ingest_pipeline = Mock(IngestionPipeline)
        community = "1234"
        collection_name = "gdrive"
        gdrive_pipeline = CustomIngestionPipeline(
            "1234", collection_name=collection_name, testing=True
        )
        docs = [
            Document(
                id_="b049e7cf-3279-404b-b324-9776fe1cf60b",
                text="""A banana is an elongated""",
            ),
            Document(
                id_="3b3033c0-7e37-493c-8b4c-fd51f754a59a",
                text="""Musa species are native to tropical Indomalaya and Australia""",
            ),
        ]

        processed_result = gdrive_pipeline.run_pipeline(docs)
        ingest_pipeline.run.return_value = processed_result
        self.assertEqual(len(processed_result), 2)

    def test_load_pipeline_run_exception(self):
        gdrive_pipeline = CustomIngestionPipeline(
            "1234", collection_name="gdrive", testing=True
        )
        gdrive_pipeline.run_pipeline = Mock()
        gdrive_pipeline.run_pipeline.side_effect = Exception("Test Exception")
        docs = ["ww"]
        with self.assertRaises(Exception) as context:
            gdrive_pipeline.run_pipeline(docs)
        self.assertEqual(str(context.exception), "Test Exception")
        gdrive_pipeline.run_pipeline.assert_called_with(docs)
