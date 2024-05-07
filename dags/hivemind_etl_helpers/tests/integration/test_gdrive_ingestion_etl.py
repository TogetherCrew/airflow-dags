from typing import List
import unittest
from unittest.mock import Mock

import psycopg2
from hivemind_etl_helpers.gdrive_ingestion_etl import GoogleDriveIngestionPipeline
from hivemind_etl_helpers.src.db.gdrive.db_utils import setup_db
from llama_index.core import MockEmbedding
from llama_index.core.ingestion import IngestionPipeline
from llama_index.core.schema import Document
from llama_index_client import TextNode
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
                id_='9b79aef5-6dae-41f7-93ea-7f1ee3f63725',
                embedding=[
                    0.5,
                    0.5,
                    0.5,
                    0.5,
                    0.5
                ],
                metadata={}, 
                excluded_embed_metadata_keys=[], 
                excluded_llm_metadata_keys=[]),
            TextNode(
                id_='a7dd4cae-517f-4bed-9038-b075406f0d3c',
                embedding=[
                    0.5,
                    0.5,
                    0.5,
                    0.5,
                    0.5
                ],
                metadata={}, excluded_embed_metadata_keys=[], excluded_llm_metadata_keys=[],
            )
        ]

        processed_result = gdrive_pipeline.run_pipeline(docs)
        self.assertEqual(len(processed_result), 2)
        self.assertEqual(processed_result[0].id_, expected_return[0].id)
        self.assertEqual(processed_result[1].id_, expected_return[1].id)
