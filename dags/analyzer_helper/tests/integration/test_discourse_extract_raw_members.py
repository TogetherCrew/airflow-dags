import unittest
from datetime import datetime

from analyzer_helper.discourse.extract_raw_members import ExtractRawMembers
from github.neo4j_storage.neo4j_connection import Neo4jConnection


class TestExtractRawMembers(unittest.TestCase):
    def setUp(self):
        self.neo4jConnection = Neo4jConnection()
        self.driver = self.neo4jConnection.connect_neo4j()
        self.test_forum_endpoint = "https://test-forum.discourse.org"
        self.platform_id = "test_platform"
        self.extractor = ExtractRawMembers(self.test_forum_endpoint, self.platform_id)
        self.rawmembers_collection = self.extractor.rawmembers_collection

        self.rawmembers_collection.insert_many(
            [
                {
                    "id": 1,
                    "is_bot": False,
                    "joined_at": datetime(2023, 7, 1),
                    "left_at:": None,
                    "options": {},
                },
                {
                    "id": 2,
                    "is_bot": False,
                    "joined_at": datetime(2023, 2, 2),
                    "left_at": None,
                    "options": {},
                },
            ]
        )

        with self.driver.session() as session:
            result_forum = session.run(
                """
                CREATE (f:DiscourseForum {endpoint: $forum_endpoint})
                RETURN id(f) AS id
                """,
                forum_endpoint=self.test_forum_endpoint,
            )
            self.forum_id = result_forum.single()["id"]
            # Create user1 and relate to forum
            result1 = session.run(
                """
                MATCH (f:DiscourseForum {endpoint: $forum_endpoint})
                CREATE (u:DiscourseUser {id: 'user1', avatarTemplate: 'avatar1', createdAt: '2023-07-01'})
                -[:HAS_JOINED]->(f)
                CREATE (u)-[:HAS_BADGE]->(:Badge {id: 'badge1'})
                RETURN id(u) AS id
                """,
                forum_endpoint=self.test_forum_endpoint,
            )
            self.user1_id = result1.single()["id"]
            # Create user2 and relate to forum
            result2 = session.run(
                """
                MATCH (f:DiscourseForum {endpoint: $forum_endpoint})
                CREATE (u:DiscourseUser {id: 'user2', avatarTemplate: 'avatar2', createdAt: '2023-07-02'})
                -[:HAS_JOINED]->(f)
                CREATE (u)-[:HAS_BADGE]->(:Badge {id: 'badge2'})
                RETURN id(u) AS id
                """,
                forum_endpoint=self.test_forum_endpoint,
            )
            self.user2_id = result2.single()["id"]

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
                "joined_at": "2023-07-01",
            },
            {
                "id": "user2",
                "joined_at": "2023-07-02",
            },
        ]
        self.assertEqual(result, expected_result)

    def test_extract_recompute(self):
        result = self.extractor.extract(recompute=True)
        expected_result = [
            {
                "id": "user1",
                "joined_at": "2023-07-01",
            },
            {
                "id": "user2",
                "joined_at": "2023-07-02",
            },
        ]
        self.assertEqual(result, expected_result)
        
    def test_extract_without_recompute(self):
        result = self.extractor.extract(
            recompute=False,
        )
        expected_result = [
            {
                "id": "user2",
                "joined_at": "2023-07-02",
            }
        ]
        self.assertEqual(result, expected_result)
