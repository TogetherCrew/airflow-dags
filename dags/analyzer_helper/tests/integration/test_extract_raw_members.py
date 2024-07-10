import unittest
from analyzer_helper.discourse.extract_raw_members import ExtractRawMembers
from github.neo4j_storage.neo4j_connection import Neo4jConnection

class TestExtractRawMembers(unittest.TestCase):

    def setUp(cls):
        cls.neo4jConnection = Neo4jConnection()
        cls.driver = cls.neo4jConnection.connect_neo4j()
        cls.extractor = ExtractRawMembers()

        with cls.driver.session() as session:
            result1 = session.run(
                """
                CREATE (u:DiscourseUser {id: 'user1', avatarTemplate: 'avatar1', createdAt: '2023-07-01'})
                -[:HAS_BADGE]->(:Badge {id: 'badge1'})
                RETURN id(u) AS id
                """
            )
            cls.user1_id = result1.single()["id"]

            result2 = session.run(
                """
                CREATE (u:DiscourseUser {id: 'user2', avatarTemplate: 'avatar2', createdAt: '2023-07-02'})
                -[:HAS_BADGE]->(:Badge {id: 'badge2'})
                RETURN id(u) AS id
                """
            )
            cls.user2_id = result2.single()["id"]

    def tearDown(cls):
        with cls.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        cls.extractor.close()
        cls.driver.close()

    def test_fetch_member_details(self):
        member_details = self.extractor.fetch_member_details()
        
        expected_members = [
            {
                'id': self.user1_id,
                'avatar': 'avatar1',
                'createdAt': '2023-07-01',
                'badgeIds': ['badge1'],
            },
            {
                'id': self.user2_id,
                'avatar': 'avatar2',
                'createdAt': '2023-07-02',
                'badgeIds': ['badge2']
            }
        ]

        self.assertEqual(len(member_details), len(expected_members))
        for member in member_details:
            self.assertIn(member, expected_members)