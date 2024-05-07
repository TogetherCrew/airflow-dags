import unittest
from unittest.mock import Mock

from llama_index_client import TextNode
import psycopg2
from dags.hivemind_etl_helpers.gdrive_ingestion_etl import GoogleDriveIngestionPipeline
from dags.hivemind_etl_helpers.src.db.gdrive.db_utils import setup_db
from llama_index.core import MockEmbedding
from llama_index.core.ingestion import IngestionPipeline
from llama_index.core.schema import Document
from tc_hivemind_backend.db.credentials import load_postgres_credentials


class TestGoogleDriveIngestionPipeline(unittest.TestCase):
    def setUpDB(self, community: str):
        db_name = f"community_{community}"
        creds = load_postgres_credentials()
        setup_db(community)
        self.postgres_conn = psycopg2.connect(
            dbname=db_name,
            user=creds["user"],
            password=creds["password"],
            host=creds["host"],
            port=creds["port"],
        )

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

        expected_return = [
            TextNode(
                id_="b049e7cf-3279-404b-b324-9776fe1cf60b",
                embedding=[
                    0.026794434,
                    -0.0103302,
                    -0.004966736,
                    -0.006626129,
                    -0.013679504,
                ],
            ),
            TextNode(
                id_="3b3033c0-7e37-493c-8b4c-fd51f754a59a",
                embedding=[
                    0.026794434,
                    -0.0103302,
                    -0.004966736,
                    -0.006626129,
                    -0.013679504,
                ],
            )
        ]

        ingest_pipeline.run.return_value = expected_return
        processed_result = gdrive_pipeline.run_pipeline(docs)
        self.assertEqual(len(processed_result), 2)
        self.assertIsInstance(processed_result, TextNode())
