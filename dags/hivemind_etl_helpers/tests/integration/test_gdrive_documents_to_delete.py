from datetime import datetime
from unittest import TestCase

import psycopg2
from hivemind_etl_helpers.src.db.gdrive.db_utils import setup_db
from hivemind_etl_helpers.src.utils.check_documents import check_documents
from hivemind_etl_helpers.src.utils.credentials import load_postgres_credentials
from hivemind_etl_helpers.src.utils.pg_vector_access import PGVectorAccess
from llama_index import Document


class TestGdriveDocumentsToDelete(TestCase):
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

    def test_empty_delete_data(self):
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

    def test_single_doc_update_filled_database(self):
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
            )
        ]
        pg_vector.save_documents(
            documents,
        )

        documents = [
            Document(
                text="sample test for data fetching!",
                metadata={
                    "file id": "9834ujiojfa09e",
                    # updating the hour
                    "modified at": datetime(2023, 11, 14, 16, 33).strftime(
                        "'%Y-%m-%d %H:%M:%S'"
                    ),
                },
            )
        ]

        docs_saving, doc_fid_deleting = check_documents(
            documents=documents,
            community_id=community_id,
            identifier="file id",
            date_field="modified at",
        )

        self.assertEqual(docs_saving, documents)
        self.assertEqual(doc_fid_deleting, ["9834ujiojfa09e"])

    def test_multiple_doc_update_filled_database(self):
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

        documents = [
            Document(
                text="sample test for data fetching!",
                metadata={
                    "file id": "9834ujiojfa09e",
                    # update the month
                    "modified at": datetime(2023, 12, 12, 16, 33).strftime(
                        "'%Y-%m-%d %H:%M:%S'"
                    ),
                },
            ),
            Document(
                text="sample test 2 for data fetching!",
                metadata={
                    "file id": "9834ujiojfa09e1111111",
                    # update the seconds
                    "modified at": datetime(2022, 11, 12, 16, 30).strftime(
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
        self.assertEqual(doc_fid_deleting, ["9834ujiojfa09e", "9834ujiojfa09e1111111"])

    def test_multiple_doc_input_partially_updated_docs(self):
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

        # updating just the second one
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
                    # updating the year
                    "modified at": datetime(2023, 11, 12, 16, 20).strftime(
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

        self.assertEqual(docs_saving, [documents[1]])
        self.assertEqual(doc_fid_deleting, ["9834ujiojfa09e1111111"])