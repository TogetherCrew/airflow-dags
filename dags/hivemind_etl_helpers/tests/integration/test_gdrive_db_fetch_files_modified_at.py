from datetime import datetime
from unittest import TestCase

import psycopg2
import pytest
from hivemind_etl_helpers.src.db.gdrive.db_utils import fetch_files_date_field, setup_db
from llama_index.core import Document
from tc_hivemind_backend.db.credentials import load_postgres_credentials
from tc_hivemind_backend.pg_vector_access import PGVectorAccess


@pytest.mark.skip(reason="GDrive ETL is not finished!")
class TestFetchGdriveFileIds(TestCase):
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

    def delete_previous_data(self, table: str):
        with self.postgres_conn.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS data_{table};")
        self.postgres_conn.commit()

    def test_file_fetching_empty_data(self):
        community = "1234"
        table_name = "gdrive"

        self.setUpDB(community)

        self.delete_previous_data(table_name)

        results = fetch_files_date_field(
            ["9834ujiojfa09e"],
            community,
            identifier="file id",
            date_field="modified at",
        )

        self.assertIsInstance(results, dict)
        self.assertEqual(results, {})

    def test_single_file_fetching(self):
        community = "1234"
        table_name = "gdrive"

        self.setUpDB(community)

        self.delete_previous_data(table_name)
        pg_vector = PGVectorAccess(
            table_name=table_name, dbname=f"community_{community}", testing=True
        )

        doc = Document(
            text="sample test for data fetching!",
            metadata={
                "file id": "9834ujiojfa09e",
                "modified at": datetime(2023, 11, 12, 16, 33).strftime(
                    "'%Y-%m-%d %H:%M:%S'"
                ),
            },
        )
        pg_vector.save_documents(
            [doc],
        )
        results = fetch_files_date_field(
            ["9834ujiojfa09e"],
            community,
            identifier="file id",
            date_field="modified at",
        )

        self.assertIsInstance(results, dict)
        self.assertEqual(results, {"9834ujiojfa09e": datetime(2023, 11, 12, 16, 33)})

    def test_multiple_file_fetching(self):
        community = "1234"
        table_name = "gdrive"

        self.setUpDB(community)

        self.delete_previous_data(table_name)
        pg_vector = PGVectorAccess(
            table_name=table_name, dbname=f"community_{community}", testing=True
        )

        documents = [
            Document(
                text="sample test for data fetching!",
                metadata={
                    "file id": "9834ujiojfa09e",
                    "modified at": datetime(2023, 11, 12, 16, 33).strftime(
                        "'%Y-%m-%d %H:%M:%S'"
                    ),
                },
            ),
            Document(
                text="sample test 2 for data fetching!",
                metadata={
                    "file id": "9834ujiojfa09e1111111",
                    "modified at": datetime(2022, 11, 12, 16, 20).strftime(
                        "'%Y-%m-%d %H:%M:%S'"
                    ),
                },
            ),
        ]

        pg_vector.save_documents(
            documents,
            disable_embedding=True,
        )
        results = fetch_files_date_field(
            ["9834ujiojfa09e", "9834ujiojfa09e1111111"],
            community,
            identifier="file id",
            date_field="modified at",
        )

        self.assertIsInstance(results, dict)
        self.assertEqual(
            results,
            {
                "9834ujiojfa09e": datetime(2023, 11, 12, 16, 33),
                "9834ujiojfa09e1111111": datetime(2022, 11, 12, 16, 20),
            },
        )
