import unittest
from unittest.mock import Mock

from hivemind_etl_helpers.gdrive_ingestion_etl import GoogleDriveIngestionPipeline
from hivemind_etl_helpers.src.db.gdrive.db_utils import setup_db
from llama_index.core import MockEmbedding
from llama_index.core.ingestion import IngestionPipeline
from llama_index.core.schema import Document
from tc_hivemind_backend.db.credentials import load_postgres_credentials


class TestGoogleDriveIngestionPipeline(unittest.TestCase):
    def setUpDB(self, community: str):
        self.db_name = f"community_{community}"
        self.creds = load_postgres_credentials()
        setup_db(community)
        self.db_config = {
            "database": self.db_name,
            "user": self.creds["user"],
            "password": self.creds["password"],
            "host": self.creds["host"],
            "port": self.creds["port"],
        }

    def test_run_pipeline(self):
        ingest_pipeline = Mock(IngestionPipeline)
        community = "1234"
        self.setUpDB(community)
        gdrive_pipeline = GoogleDriveIngestionPipeline("1234")
        gdrive_pipeline.cohere_model = MockEmbedding(embed_dim=1024)
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
        ingest_pipeline = Mock(IngestionPipeline)
        community = "1234"
        self.setUpDB(community)
        gdrive_pipeline = GoogleDriveIngestionPipeline("1234")
        gdrive_pipeline.cohere_model = MockEmbedding(embed_dim=1024)
        docs = ["ww"]

        processed_result = gdrive_pipeline.run_pipeline(docs)
        ingest_pipeline.side_effect = Exception("Test Exception")
        self.assertIsNone(processed_result)
