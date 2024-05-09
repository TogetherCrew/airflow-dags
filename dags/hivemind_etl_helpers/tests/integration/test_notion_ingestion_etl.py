import unittest
from unittest.mock import Mock

import psycopg2
from hivemind_etl_helpers.src.db.gdrive.db_utils import setup_db
from llama_index.core.schema import Document
from tc_hivemind_backend.db.credentials import load_postgres_credentials

from dags.hivemind_etl_helpers.ingestion_pipeline import (
    CustomIngestionPipeline
)


class TestNotionIngestionPipeline(unittest.TestCase):
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
        ingest_pipeline = Mock(CustomIngestionPipeline)
        community = "1312"
        self.setUpDB(community)
        table_name = "notion"
        notion_ingestion_pipeline = CustomIngestionPipeline(
            community_id="1312", table_name=table_name, testing=True
        )
        docs = [
            Document(
                id_="8307b4b8-1dc5-4d1b-a5b5-084e18bc4046",
                text="""An apple is a rounded fruit""",
            ),
            Document(
                id_="3b3033c0-7e37-493c-8b4c-fd51f754a59a",
                text="""Citrus fruits are indigenous to subtropical and tropical regions of Asia""",
            ),
        ]

        processed_result = notion_ingestion_pipeline.run_pipeline(docs)
        ingest_pipeline.run.return_value = processed_result
        self.assertEqual(len(processed_result), 2)
