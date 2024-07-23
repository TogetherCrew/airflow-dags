from datetime import datetime
import unittest
from analyzer_helper.discourse.extract_raw_members import ExtractRawMembers
from github.neo4j_storage.neo4j_connection import Neo4jConnection

class TestExtractRawMembers(unittest.TestCase):

    def setUp(cls):
        cls.neo4jConnection = Neo4jConnection()
        cls.driver = cls.neo4jConnection.connect_neo4j()
        cls.test_forum_endpoint = "https://test-forum.discourse.org"
        cls.platform_id = "test_platform"
        cls.extractor = ExtractRawMembers(cls.test_forum_endpoint, cls.platform_id)
        cls.rawmembers_collection = cls.extractor.rawmembers_collection
        
        cls.rawmembers_collection.insert_many([
            {'id': 1, 'joined_at': datetime(2023, 7, 1), 'avatar': 'avatar1.png', 'badgeIds': [1, 2]},
            {'id': 2, 'joined_at': datetime(2023, 2, 2), 'avatar': 'avatar2.png', 'badgeIds': [3]}
        ])

        with cls.driver.session() as session:

            result_forum = session.run(
                """
                CREATE (f:DiscourseForum {endpoint: $forum_endpoint})
                RETURN id(f) AS id
                """,
                forum_endpoint=cls.test_forum_endpoint
            )
            cls.forum_id = result_forum.single()["id"]
            
            # Create user1 and relate to forum
            result1 = session.run(
                """
                MATCH (f:DiscourseForum {endpoint: $forum_endpoint})
                CREATE (u:DiscourseUser {id: 'user1', avatarTemplate: 'avatar1', createdAt: '2023-07-01'})
                -[:HAS_JOINED]->(f)
                CREATE (u)-[:HAS_BADGE]->(:Badge {id: 'badge1'})
                RETURN id(u) AS id
                """,
                forum_endpoint=cls.test_forum_endpoint
            )
            cls.user1_id = result1.single()["id"]

            # Create user2 and relate to forum
            result2 = session.run(
                """
                MATCH (f:DiscourseForum {endpoint: $forum_endpoint})
                CREATE (u:DiscourseUser {id: 'user2', avatarTemplate: 'avatar2', createdAt: '2023-07-02'})
                -[:HAS_JOINED]->(f)
                CREATE (u)-[:HAS_BADGE]->(:Badge {id: 'badge2'})
                RETURN id(u) AS id
                """,
                forum_endpoint=cls.test_forum_endpoint
            )
            cls.user2_id = result2.single()["id"]

    def tearDown(cls):
        with cls.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        cls.extractor.close()
        cls.driver.close()
        cls.rawmembers_collection.delete_many({})

    def test_fetch_member_details(self):
        member_details = self.extractor.fetch_member_details()
        
        expected_members = [
            {
                'id': 'user1',
                'avatar': 'avatar1',
                'joined_at': '2023-07-01',
                'badgeIds': ['badge1'],
            },
            {
                'id': 'user2',
                'avatar': 'avatar2',
                'joined_at': '2023-07-02',
                'badgeIds': ['badge2']
            }
        ]

        self.assertEqual(len(member_details), len(expected_members))
        for member in member_details:
            self.assertIn(member, expected_members)

    def test_extract_recompute(cls):
        result = cls.extractor.extract(recompute=True)
        expected_members = [
            {
                'id': 'user1',
                'avatar': 'avatar1',
                'joined_at': "2023-07-01",
                'badgeIds': ['badge1'],
            },
            {
                'id': 'user2',
                'avatar': 'avatar2',
                'joined_at': '2023-07-02',
                'badgeIds': ['badge2']
            }
        ]
        cls.assertEqual(result, expected_members)
        
    def test_extract_without_recompute(self):
        result = self.extractor.extract(recompute=False,)
        expected_result = [
            {'id': 'user1', 'avatar': 'avatar1', 'joined_at': '2023-07-01', 'badgeIds': []},
            {'id': 'user2', 'avatar': 'avatar2', 'joined_at': '2023-07-02', 'badgeIds': ['badge2']}
        ]
        self.assertEqual(result, expected_result)