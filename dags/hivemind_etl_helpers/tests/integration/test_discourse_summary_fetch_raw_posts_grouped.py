import unittest
from datetime import datetime

import neo4j
from hivemind_etl_helpers.src.db.discourse.fetch_raw_posts import (
    fetch_raw_posts_grouped,
)
from hivemind_etl_helpers.src.utils.neo4j import Neo4jConnection


class TestFetchRawPostsGrouped(unittest.TestCase):
    def test_fetch_all_posts(self):
        neo4j_ops = Neo4jConnection().neo4j_ops
        forum_endpoint = "636363636363"

        neo4j_ops.neo4j_driver.execute_query(
            """
            MATCH (n) DETACH DELETE (n)
            """
        )
        neo4j_ops.neo4j_driver.execute_query(
            """
            CREATE (p:DiscoursePost)
            SET
                p.endpoint = "636363636363",
                p.raw = "texttexttext of post 1",
                p.topicId = 1,
                p.id = 100,
                p.createdAt = '2022-01-01T00:00:00.000Z',
                p.updatedAt = '2022-01-01T01:00:00.000Z'
            WITH p
            CREATE (a:DiscourseUser) -[:POSTED]->(p)
            SET
                a.username = "user#1",
                a.name = "user1",
                a.trustLevel = 4
            """
        )
        neo4j_ops.neo4j_driver.execute_query(
            """
            CREATE (t:DiscourseTopic)
            SET
                t.title = "topic#1",
                t.id = 1
            WITH t
            CREATE (c:DiscourseCategory {name: 'SampleCat1'})-[:HAS_TOPIC]->(t)
            """
        )
        neo4j_ops.neo4j_driver.execute_query(
            """
            CREATE (p:DiscoursePost)
            SET
                p.endpoint = "636363636363",
                p.raw = "texttexttext of post 2",
                p.topicId = 2,
                p.id = 101,
                p.createdAt = '2022-01-04T00:01:00.000Z',
                p.updatedAt = '2022-01-04T01:01:00.000Z'
            WITH p
            CREATE (a:DiscourseUser) -[:POSTED]->(p)
            SET
                a.username = "user#2",
                a.name = "user2",
                a.trustLevel = 1
            WITH p
            MATCH (p2:DiscoursePost {id: 100})
            CREATE (p)-[:REPLIED_TO]->(p2);
            """
        )
        neo4j_ops.neo4j_driver.execute_query(
            """
            CREATE (t:DiscourseTopic)
            SET
                t.title = "topic#2",
                t.id = 2
            """
        )

        result = fetch_raw_posts_grouped(forum_endpoint)
        self.assertIsInstance(result, list)
        self.assertTrue(
            all(isinstance(record, neo4j._data.Record) for record in result)
        )
        self.assertEqual(len(result), 2)

    def test_fetch_posts_from_date(self):
        neo4j_ops = Neo4jConnection().neo4j_ops
        forum_endpoint = "636363636363"

        neo4j_ops.neo4j_driver.execute_query(
            """
            MATCH (n) DETACH DELETE (n)
            """
        )
        neo4j_ops.neo4j_driver.execute_query(
            """
            CREATE (p:DiscoursePost)
            SET
                p.endpoint = "636363636363",
                p.raw = "texttexttext of post 1",
                p.topicId = 1,
                p.id = 100,
                p.createdAt = '2022-01-01T00:00:00.000Z',
                p.updatedAt = '2022-01-01T01:00:00.000Z'
            WITH p
            CREATE (a:DiscourseUser) -[:POSTED]->(p)
            SET
                a.username = "user#1",
                a.name = "user1",
                a.trustLevel = 4
            """
        )
        neo4j_ops.neo4j_driver.execute_query(
            """
            CREATE (t:DiscourseTopic)
            SET
                t.title = "topic#1",
                t.id = 1
            WITH t
            CREATE (c:DiscourseCategory {name: 'SampleCat1'})-[:HAS_TOPIC]->(t)
            """
        )
        neo4j_ops.neo4j_driver.execute_query(
            """
            CREATE (p:DiscoursePost)
            SET
                p.endpoint = "636363636363",
                p.raw = "texttexttext of post 2",
                p.topicId = 2,
                p.id = 101,
                p.createdAt = '2022-01-04T00:01:00.000Z',
                p.updatedAt = '2022-01-04T01:01:00.000Z'
            WITH p
            CREATE (a:DiscourseUser) -[:POSTED]->(p)
            SET
                a.username = "user#2",
                a.name = "user2",
                a.trustLevel = 1
            WITH p
            MATCH (p2:DiscoursePost {id: 100})
            CREATE (p)-[:REPLIED_TO]->(p2);
            """
        )
        neo4j_ops.neo4j_driver.execute_query(
            """
            CREATE (t:DiscourseTopic)
            SET
                t.title = "topic#2",
                t.id = 2
            """
        )

        from_date = datetime(2022, 1, 2)
        result = fetch_raw_posts_grouped(forum_endpoint, from_date)
        self.assertIsInstance(result, list)
        self.assertTrue(
            all(isinstance(record, neo4j._data.Record) for record in result)
        )
        self.assertEqual(len(result), 1)

    def test_fetch_posts_invalid_forum_id(self):
        neo4j_ops = Neo4jConnection().neo4j_ops
        forum_endpoint = "invalid_forum"

        neo4j_ops.neo4j_driver.execute_query(
            """
            MATCH (n) DETACH DELETE (n)
            """
        )
        neo4j_ops.neo4j_driver.execute_query(
            """
            CREATE (p:DiscoursePost)
            SET
                p.endpoint = "636363636363",
                p.raw = "texttexttext of post 1",
                p.topicId = 1,
                p.id = 100,
                p.createdAt = '2022-01-01T00:00:00.000Z',
                p.updatedAt = '2022-01-01T01:00:00.000Z'
            WITH p
            CREATE (a:DiscourseUser) -[:POSTED]->(p)
            SET
                a.username = "user#1",
                a.name = "user1",
                a.trustLevel = 4
            """
        )
        neo4j_ops.neo4j_driver.execute_query(
            """
            CREATE (t:DiscourseTopic)
            SET
                t.title = "topic#1",
                t.id = 1
            WITH t
            CREATE (c:DiscourseCategory {name: 'SampleCat1'})-[:HAS_TOPIC]->(t)
            """
        )
        neo4j_ops.neo4j_driver.execute_query(
            """
            CREATE (p:DiscoursePost)
            SET
                p.endpoint = "636363636363",
                p.raw = "texttexttext of post 2",
                p.topicId = 2,
                p.id = 101,
                p.createdAt = '2022-01-04T00:01:00.000Z',
                p.updatedAt = '2022-01-04T01:01:00.000Z'
            WITH p
            CREATE (a:DiscourseUser) -[:POSTED]->(p)
            SET
                a.username = "user#2",
                a.name = "user2",
                a.trustLevel = 1
            WITH p
            MATCH (p2:DiscoursePost {id: 100})
            CREATE (p)-[:REPLIED_TO]->(p2);
            """
        )
        neo4j_ops.neo4j_driver.execute_query(
            """
            CREATE (t:DiscourseTopic)
            SET
                t.title = "topic#2",
                t.id = 2
            """
        )

        result = fetch_raw_posts_grouped(forum_endpoint)
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 0)

    def test_fetch_posts_empty_forum(self):
        neo4j_ops = Neo4jConnection().neo4j_ops
        neo4j_ops.neo4j_driver.execute_query(
            """
            MATCH (n) DETACH DELETE (n)
            """
        )

        forum_endpoint = "636363636363"
        result = fetch_raw_posts_grouped(forum_endpoint)
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 0)
