import unittest
from unittest.mock import Mock

import psycopg2
from llama_index.core.ingestion import IngestionPipeline
from llama_index.core.schema import Document
from tc_hivemind_backend.db.credentials import load_postgres_credentials

from dags.hivemind_etl_helpers.gdrive_ingestion_etl import GoogleDriveIngestionPipeline
from dags.hivemind_etl_helpers.src.db.gdrive.db_utils import setup_db


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
        my_object = GoogleDriveIngestionPipeline("1234")
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

        ingest_pipeline.run.return_value = docs
        processed_result = my_object.run_pipeline(docs)
        self.assertEqual(len(processed_result), 0)
