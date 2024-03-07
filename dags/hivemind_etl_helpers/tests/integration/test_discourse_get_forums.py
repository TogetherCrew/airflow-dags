from unittest import TestCase

import neo4j
from hivemind_etl_helpers.src.db.discourse.utils.get_forums import get_forum_uuid
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
        forum_endpoint = "example.endpoint.com"
        forum_uuid = get_forum_uuid(forum_endpoint=forum_endpoint)

        self.assertEqual(forum_uuid, [])

    def test_get_single_forum(self):
        self.setUp()
        forum_endpoint = "example.endpoint.com"

        self.neo4j.neo4j_ops.neo4j_driver.execute_query(
            """
            CREATE (f:DiscourseForum {uuid: 'tt18yasop', endpoint: $forum_endpoint})
            """,
            forum_endpoint=forum_endpoint,
        )
        forum_uuid = get_forum_uuid(forum_endpoint=forum_endpoint)

        self.assertIsInstance(forum_uuid, list)
        self.assertEqual(len(forum_uuid), 1)
        self.assertIsInstance(forum_uuid[0], neo4j._data.Record)
        self.assertEqual(
            forum_uuid,
            [neo4j._data.Record({"uuid": "tt18yasop"})],
        )
        self.assertEqual(forum_uuid[0]["uuid"], "tt18yasop")
