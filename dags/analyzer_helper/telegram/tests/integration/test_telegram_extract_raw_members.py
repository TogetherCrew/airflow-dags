import datetime
import unittest

from analyzer_helper.telegram.extract_raw_members import ExtractRawMembers
from github.neo4j_storage.neo4j_connection import Neo4jConnection


class TestExtractRawMembers(unittest.TestCase):
    def setUp(self):
        self.neo4jConnection = Neo4jConnection()
        self.driver = self.neo4jConnection.connect_neo4j()
        self.chat_id = 12345665432
        self.platform_id = "telegram_test_platform"
        self.extractor = ExtractRawMembers(self.chat_id, self.platform_id)
        self.rawmembers_collection = self.extractor.rawmembers_collection

        self.rawmembers_collection.insert_many(
            [
                {
                    "id": 1.0,
                    "is_bot": False,
                    "joined_at": datetime.datetime(2023, 7, 1),
                    "left_at": None,
                    "username": "user1",
                    "first_name": "user",
                    "last_name": "1",
                },
                {
                    "id": 2.0,
                    "is_bot": False,
                    "joined_at": datetime.datetime(2023, 7, 3),
                    "left_at": None,
                    "username": "user2",
                    "first_name": "user",
                    "last_name": "2",
                },
            ]
        )

        with self.driver.session() as session:
            session.run(
                """
                CREATE (c:TGChat {id: $chat_id})
                """,
                chat_id=self.chat_id,
            )
            session.run(
                """
                MATCH (c:TGChat {id: $chat_id})
                CREATE (u:TGUser {
                    id: 1.0,
                    is_bot: false,
                    created_at: $created_at,
                    first_name: "user",
                    last_name: "1",
                    username: "user1"
                })
                -[:JOINED {date: $joined_date}]->(c)
                CREATE (u)-[:LEFT {date: $left_date}]->(c)
                RETURN id(u) AS id
                """,
                chat_id=self.chat_id,
                created_at=1688162400.0,  # 2023, 7, 1
                joined_date=1688162400.0,  # 2023, 7, 1
                left_date=1688248800.0,  # 2023, 7, 2
            )
            session.run(
                """
                MATCH (c:TGChat {id: $chat_id})
                CREATE (u:TGUser {
                    id: 2.0,
                    is_bot: false,
                    created_at: $created_at,
                    first_name: "user",
                    last_name: "2",
                    username: "user2"
                }
                )
                -[:JOINED {date: $joined_date}]->(c)
                RETURN id(u) AS id
                """,
                chat_id=self.chat_id,
                created_at=1688342400.0,  # 2023, 7, 3
                joined_date=1688342400.0,  # 2023, 7, 3
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
                "id": 1.0,
                "is_bot": False,
                "joined_at": 1688162400.0,
                "left_at": 1688248800.0,
                "username": "user1",
                "first_name": "user",
                "last_name": "1",
            },
            {
                "id": 2.0,
                "is_bot": False,
                "joined_at": 1688342400.0,
                "left_at": None,
                "username": "user2",
                "first_name": "user",
                "last_name": "2",
            },
        ]
        self.assertEqual(len(result), 2)
        for res in result:
            self.assertIn(res, expected_result)

    def test_extract_recompute(self):
        result = self.extractor.extract(recompute=True)

        expected_result = [
            {
                "id": 1.0,
                "is_bot": False,
                "joined_at": 1688162400.0,
                "left_at": 1688248800.0,
                "username": "user1",
                "first_name": "user",
                "last_name": "1",
            },
            {
                "id": 2.0,
                "is_bot": False,
                "joined_at": 1688342400.0,
                "left_at": None,
                "username": "user2",
                "first_name": "user",
                "last_name": "2",
            },
        ]
        self.assertEqual(len(result), 2)
        for res in result:
            self.assertIn(res, expected_result)

    def test_extract_without_recompute(self):
        result = self.extractor.extract(recompute=False)
        # the users data was available before
        self.assertEqual(len(result), 0)
