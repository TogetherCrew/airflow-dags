from unittest import TestCase

import neo4j

from hivemind_etl_helpers.src.db.discourse.utils.get_forums import get_forums
from hivemind_etl_helpers.src.utils.neo4j import Neo4jConnection


class TestGetDiscourseForums(TestCase):
    def setUp(self):
        neo4j = Neo4jConnection()
        neo4j.neo4j_ops.neo4j_driver.execute_query(
            """
            MATCH (n) DETACH DELETE (n)
            """
        )

        self.neo4j = neo4j

    def test_get_forums_empty_forum(self):
        self.setUp()
        community_id = "1234"
        forums = get_forums(community_id=community_id)

        self.assertEqual(forums, [])

    def test_get_single_forum(self):
        self.setUp()
        community_id = "1234"

        self.neo4j.neo4j_ops.neo4j_driver.execute_query(
            """
            CREATE (c: Community {id: $communityId})
            WITH c
            CREATE (f:DiscourseForum {uuid: 'tt18yasop', endpoint: 'test.example.com'})
            WITH c, f
            CREATE (f)-[:IS_WITHIN]->(c)
            """,
            communityId=community_id,
        )
        forums = get_forums(community_id=community_id)

        self.assertIsInstance(forums, list)
        self.assertEqual(len(forums), 1)
        self.assertIsInstance(forums[0], neo4j._data.Record)
        self.assertEqual(
            forums,
            [neo4j._data.Record({"uuid": "tt18yasop", "endpoint": "test.example.com"})],
        )

    def test_get_multiple_forums(self):
        self.setUp()
        community_id = "1234"

        self.neo4j.neo4j_ops.neo4j_driver.execute_query(
            """
            CREATE (c: Community {id: $communityId})
            WITH c
            CREATE (f:DiscourseForum {uuid: 'tt18yasop', endpoint: 'test.example.com'})
            CREATE (f2:DiscourseForum {uuid: 'tt18yasop2', endpoint: 'test2.example.com'})
            CREATE (f3:DiscourseForum {uuid: 'tt18yasop3', endpoint: 'test3.example.com'})
            WITH c, f, f2, f3
            CREATE (f)-[:IS_WITHIN]->(c)
            CREATE (f2)-[:IS_WITHIN]->(c)
            CREATE (f3)-[:IS_WITHIN]->(c)
            """,
            communityId=community_id,
        )
        forums = get_forums(community_id=community_id)

        self.assertIsInstance(forums, list)
        self.assertEqual(len(forums), 3)
        for forum in forums:
            self.assertIsInstance(forum, neo4j._data.Record)

        self.assertEqual(
            forums,
            [
                neo4j._data.Record(
                    {"uuid": "tt18yasop", "endpoint": "test.example.com"}
                ),
                neo4j._data.Record(
                    {"uuid": "tt18yasop2", "endpoint": "test2.example.com"}
                ),
                neo4j._data.Record(
                    {"uuid": "tt18yasop3", "endpoint": "test3.example.com"}
                ),
            ],
        )
