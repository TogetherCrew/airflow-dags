import unittest
import datetime

from github.neo4j_storage.neo4j_connection import Neo4jConnection
from analyzer_helper.telegram.extract_raw_members import ExtractRawMembers

class TestExtractRawMembers(unittest.TestCase):
    def setUp(self):
        self.neo4jConnection = Neo4jConnection()
        self.driver = self.neo4jConnection.connect_neo4j()
        self.test_chat_title = "telegram_test_group"
        self.platform_id = "telegram_test_platform"
        self.extractor = ExtractRawMembers(self.test_chat_title, self.platform_id)
        self.rawmembers_collection = self.extractor.rawmembers_collection

        self.rawmembers_collection.insert_many(
            [
                {
                    "id": 1,
                    "is_bot": False,
                    "joined_at": datetime.datetime(2023, 7, 1),
                    "left_at": None,
                    "options": {},
                },
                {
                    "id": 2,
                    "is_bot": False,
                    "joined_at": datetime.datetime(2023, 2, 2),
                    "left_at": None,
                    "options": {},
                },
            ]
        )

        with self.driver.session() as session:
            session.run(
                """
                CREATE (c:TGChat {title: $chat_title})
                """,
                chat_title=self.test_chat_title,
            )
            session.run(
                """
                MATCH (c:TGChat {title: $chat_title})
                CREATE (u:TGUser {id: 'user1', is_bot: false, created_at: $created_at})
                -[:JOINED {date: $joined_date}]->(c)
                CREATE (u)-[:LEFT {date: $left_date}]->(c)
                RETURN id(u) AS id
                """,
                chat_title=self.test_chat_title,
                created_at=1688162400.0, #2023, 7, 1
                joined_date=1688162400.0, #2023, 7, 1
                left_date=1688248800.0, #2023, 7, 2
            )
            session.run(
                """
                MATCH (c:TGChat {title: $chat_title})
                CREATE (u:TGUser {id: 'user2', is_bot: false, created_at: $created_at})
                -[:JOINED {date: $joined_date}]->(c)
                RETURN id(u) AS id
                """,
                chat_title=self.test_chat_title,
                # created_at=1675292400.0, #2023, 2, 2
                # joined_date=1675292400.0, #2023, 2, 2
                created_at=1688342400.0, #2023, 7, 3
                joined_date=1688342400.0, #2023, 7, 3
            )

    def tearDown(self):
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        self.extractor.close()
        self.driver.close()
        self.rawmembers_collection.delete_many({})

    def test_fetch_member_details(self):
        result = self.extractor.fetch_member_details()

        expected_result = [
            {
                "id": "user1",
                "is_bot": False,
                "joined_at": 1688162400.0,
                "left_at": 1688248800.0,
            },
            {
                "id": "user2",
                "is_bot": False,
                "joined_at": 1688342400.0,
                "left_at": None,
            },
        ]
        self.assertEqual(result, expected_result)

    def test_extract_recompute(self):
        result = self.extractor.extract(recompute=True)

        expected_result = [
            {
                "id": "user1",
                "is_bot": False,
                "joined_at": 1688162400.0,
                "left_at": 1688248800.0,
            },
            {
                "id": "user2",
                "is_bot": False,
                "joined_at": 1688342400.0,
                "left_at": None,
            },
        ]
        self.assertEqual(result, expected_result)

    def test_extract_without_recompute(self):
        result = self.extractor.extract(recompute=False)

        expected_result = [
            {
                "id": "user1",
                "is_bot": False,
                "joined_at": 1688162400.0,
                "left_at": 1688248800.0,
            },
            {
                "id": "user2",
                "is_bot": False,
                "joined_at": 1688342400.0,
                "left_at": None,
            },
        ]
        print("result: \n", result)
        print("expected result: \n", expected_result)
        self.assertEqual(result, expected_result)

