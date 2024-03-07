from datetime import datetime
from unittest import TestCase

import psycopg2
import pytest
from hivemind_etl_helpers.src.db.gdrive.db_utils import setup_db
from hivemind_etl_helpers.src.utils.check_documents import check_documents
from llama_index.core import Document
from tc_hivemind_backend.db.credentials import load_postgres_credentials
from tc_hivemind_backend.pg_vector_access import PGVectorAccess


@pytest.mark.skip(reason="GDrive ETL is not finished!")
class TestGdriveDocumentsToInsert(TestCase):
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

    def test_empty_data(self):
        community_id = "1234"
        table_name = "gdrive"
        self.setUpDB(community_id=community_id)
        self.delete_previous_data(table_name)

        docs_saving, doc_fid_deleting = check_documents(
            documents=[],
            community_id=community_id,
            identifier="file id",
            date_field="modified at",
        )

        self.assertEqual(docs_saving, [])
        self.assertEqual(doc_fid_deleting, [])

    def test_single_doc_input_empty_database(self):
        community_id = "1234"
        table_name = "gdrive"
        self.setUpDB(community_id=community_id)
        self.delete_previous_data(table_name)

        doc = Document(
            text="sample test for data fetching!",
            metadata={
                "file id": "9834ujiojfa09e",
                "modified at": datetime(2023, 11, 12, 16, 33).strftime(
                    "'%Y-%m-%d %H:%M:%S'"
                ),
            },
        )

        docs_saving, doc_fid_deleting = check_documents(
            documents=[doc],
            community_id=community_id,
            identifier="file id",
            date_field="modified at",
        )

        self.assertEqual(docs_saving, [doc])
        self.assertEqual(doc_fid_deleting, [])

    def test_multiple_doc_input_empty_database(self):
        community_id = "1234"
        table_name = "gdrive"
        self.setUpDB(community_id=community_id)
        self.delete_previous_data(table_name)

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

        docs_saving, doc_fid_deleting = check_documents(
            documents=documents,
            community_id=community_id,
            identifier="file id",
            date_field="modified at",
        )

        self.assertEqual(docs_saving, documents)
        self.assertEqual(doc_fid_deleting, [])

    def test_single_doc_input_filled_database(self):
        """
        in the database we have exactly the input document
        """
        community_id = "1234"
        table_name = "gdrive"
        self.setUpDB(community_id=community_id)
        self.delete_previous_data(table_name)

        pg_vector = PGVectorAccess(
            table_name=table_name, dbname=f"community_{community_id}", testing=True
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
        docs_saving, doc_fid_deleting = check_documents(
            documents=[doc],
            community_id=community_id,
            identifier="file id",
            date_field="modified at",
        )

        self.assertEqual(docs_saving, [])
        self.assertEqual(doc_fid_deleting, [])

    def test_multiple_doc_input_filled_database(self):
        """
        in the database we have exactly the input document
        """
        community_id = "1234"
        table_name = "gdrive"
        self.setUpDB(community_id=community_id)
        self.delete_previous_data(table_name)

        pg_vector = PGVectorAccess(
            table_name=table_name, dbname=f"community_{community_id}", testing=True
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
        )
        docs_saving, doc_fid_deleting = check_documents(
            documents=documents,
            community_id=community_id,
            identifier="file id",
            date_field="modified at",
        )

        self.assertEqual(docs_saving, [])
        self.assertEqual(doc_fid_deleting, [])

    def test_multiple_doc_input_partially_filled_database(self):
        """
        in the database we have just one document avaialble but 2 documents input
        """
        community_id = "1234"
        table_name = "gdrive"
        self.setUpDB(community_id=community_id)
        self.delete_previous_data(table_name)

        pg_vector = PGVectorAccess(
            table_name=table_name, dbname=f"community_{community_id}", testing=True
        )

        doc1 = Document(
            text="sample test for data fetching!",
            metadata={
                "file id": "9834ujiojfa09e",
                "modified at": datetime(2023, 11, 12, 16, 33).strftime(
                    "'%Y-%m-%d %H:%M:%S'"
                ),
            },
        )
        doc2 = Document(
            text="sample test 2 for data fetching!",
            metadata={
                "file id": "9834ujiojfa09e1111111",
                "modified at": datetime(2022, 11, 12, 16, 20).strftime(
                    "'%Y-%m-%d %H:%M:%S'"
                ),
            },
        )
        pg_vector.save_documents(
            [doc1],
        )
        docs_saving, doc_fid_deleting = check_documents(
            documents=[doc1, doc2],
            community_id=community_id,
            identifier="file id",
            date_field="modified at",
        )

        self.assertEqual(docs_saving, [doc2])
        self.assertEqual(doc_fid_deleting, [])
