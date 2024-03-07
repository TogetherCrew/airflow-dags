from datetime import datetime
from unittest import TestCase
import pytest

import psycopg2
from hivemind_etl_helpers.src.db.gdrive.db_utils import setup_db
from hivemind_etl_helpers.src.db.gdrive.delete_records import delete_records
from llama_index.core import Document
from tc_hivemind_backend.db.credentials import load_postgres_credentials
from tc_hivemind_backend.db.pg_db_utils import convert_tuple_str
from tc_hivemind_backend.pg_vector_access import PGVectorAccess


@pytest.mark.skip(reason="GDrive ETL is not finished!")
class TestDeleteRecords(TestCase):
    def setUpDB(self, community_id: str):
        db_name = f"community_{community_id}"
        creds = load_postgres_credentials()
        setup_db(community_id)
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

    def fetch_records(self, file_ids: list[str], table: str) -> list[Document]:
        query_ids = convert_tuple_str(file_ids)
        with self.postgres_conn.cursor() as cursor:
            if query_ids != "()":
                cursor.execute(
                    f"""
                    SELECT * FROM {table}
                    WHERE metadata_->>'file id' IN {query_ids};
                    """
                )
                results = cursor.fetchall()
            else:
                results = []
        return results

    def test_empty_inputs(self):
        """
        no error should be raised
        """
        community_id = "1234"
        table_name = "gdrive"

        self.setUpDB(community_id=community_id)
        self.delete_previous_data(table_name)

        delete_records(
            db_name=f"community_{community_id}",
            table_name=table_name,
            metadata_file_id=[],
        )
        results = self.fetch_records([], table_name)
        self.assertEqual(results, [])

    def test_delete_one_record(self):
        """
        no error should be raised
        """
        community_id = "1234"
        table_name = "gdrive"

        self.setUpDB(community_id=community_id)
        self.delete_previous_data(table_name)

        # saving a document
        pg_vector = PGVectorAccess(
            table_name=table_name, dbname=f"community_{community_id}", testing=True
        )

        file_id = "9834ujiojfa09e"
        documents = [
            Document(
                text="sample test for data fetching!",
                metadata={
                    "file id": file_id,
                    "modified at": datetime(2023, 11, 12, 16, 33).strftime(
                        "'%Y-%m-%d %H:%M:%S'"
                    ),
                },
            )
        ]
        pg_vector.save_documents(community_id, documents)

        delete_records(
            db_name=f"community_{community_id}",
            table_name=table_name,
            metadata_file_id=[file_id],
        )
        results = self.fetch_records(file_ids=[file_id], table=table_name)
        self.assertEqual(results, [])

    def test_delete_multiple_record(self):
        """
        no error should be raised
        """
        community_id = "1234"
        table_name = "gdrive"

        self.setUpDB(community_id=community_id)
        self.delete_previous_data(table_name)

        # saving a document
        pg_vector = PGVectorAccess(
            table_name=table_name, dbname=f"community_{community_id}", testing=True
        )

        file_id = "9834ujiojfa09e"
        file_id2 = "9834ujiojfa09e11111111"
        documents = [
            Document(
                text="sample test for data fetching!",
                metadata={
                    "file id": file_id,
                    "modified at": datetime(2023, 11, 12, 16, 33).strftime(
                        "'%Y-%m-%d %H:%M:%S'"
                    ),
                },
            ),
            Document(
                text="sample test for data fetching!",
                metadata={
                    "file id": file_id2,
                    "modified at": datetime(2023, 11, 16, 16, 33).strftime(
                        "'%Y-%m-%d %H:%M:%S'"
                    ),
                },
            ),
        ]
        pg_vector.save_documents(community_id, documents)

        delete_records(
            db_name=f"community_{community_id}",
            table_name=table_name,
            metadata_file_id=[file_id, file_id2],
        )
        results = self.fetch_records(file_ids=[file_id, file_id2], table=table_name)
        self.assertEqual(results, [])

    def test_delete_one_record_multiple_data_available(self):
        """
        no error should be raised
        """
        community_id = "1234"
        table_name = "gdrive"

        self.setUpDB(community_id=community_id)
        self.delete_previous_data(table_name)

        # saving a document
        pg_vector = PGVectorAccess(
            table_name=table_name, dbname=f"community_{community_id}", testing=True
        )

        file_id = "9834ujiojfa09e"
        file_id2 = "9834ujiojfa09e11111111"
        documents = [
            Document(
                text="sample test for data fetching!",
                metadata={
                    "file id": file_id,
                    "modified at": datetime(2023, 11, 12, 16, 33).strftime(
                        "'%Y-%m-%d %H:%M:%S'"
                    ),
                },
            ),
            Document(
                text="sample test for data fetching!",
                metadata={
                    "file id": file_id2,
                    "modified at": datetime(2023, 11, 16, 16, 33).strftime(
                        "'%Y-%m-%d %H:%M:%S'"
                    ),
                },
            ),
        ]
        pg_vector.save_documents(community_id, documents)

        # delete_records(
        #     db_name=f"community_{community_id}",
        #     table_name=table_name,
        #     metadata_file_id=[file_id]
        # )
        results = self.fetch_records(file_ids=[file_id, file_id2], table=table_name)
        # TODO: Update
        self.assertEqual(results, [documents[1]])
