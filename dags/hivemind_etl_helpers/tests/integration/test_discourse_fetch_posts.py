from datetime import datetime
from unittest import TestCase

from hivemind_etl_helpers.src.db.discourse.fetch_raw_posts import fetch_raw_posts
from hivemind_etl_helpers.src.utils.neo4j import Neo4jConnection


class TestFetchRawDiscoursePosts(TestCase):
    def test_fetch_empty_data_without_from_date(self):
        neo4j_ops = Neo4jConnection().neo4j_ops
        forum_endpoint = "1234"

        neo4j_ops.neo4j_driver.execute_query(
            """
            MATCH (n) DETACH DELETE (n)
            """
        )

        posts = fetch_raw_posts(forum_endpoint=forum_endpoint, from_date=None)

        self.assertEqual(posts, [])

    def test_fetch_empty_data_with_from_date(self):
        neo4j_ops = Neo4jConnection().neo4j_ops
        forum_endpoint = "1234"

        neo4j_ops.neo4j_driver.execute_query(
            """
            MATCH (n) DETACH DELETE (n)
            """
        )

        posts = fetch_raw_posts(
            forum_endpoint=forum_endpoint, from_date=datetime(2015, 1, 1)
        )

        self.assertEqual(posts, [])

    def test_fetch_some_data_without_from_date(self):
        neo4j_ops = Neo4jConnection().neo4j_ops
        forum_endpoint = "wwwdwadeswdpoi123"

        neo4j_ops.neo4j_driver.execute_query(
            """
            MATCH (n) DETACH DELETE (n)
            """
        )

        neo4j_ops.neo4j_driver.execute_query(
            """
            CREATE (p:DiscoursePost)
            SET
                p.endpoint = "wwwdwadeswdpoi123",
                p.raw = "texttexttext of post 1",
                p.topicId = 1,
                p.id = 100,
                p.createdAt = '2022-01-01T00:00:00.000Z',
                p.updatedAt = '2022-01-01T01:00:00.000Z',
                p.postNumber = 1.0
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
                p.endpoint = "wwwdwadeswdpoi123",
                p.raw = "texttexttext of post 2",
                p.topicId = 2,
                p.id = 101,
                p.createdAt = '2022-01-01T00:01:00.000Z',
                p.updatedAt = '2022-01-01T01:01:00.000Z',
                p.postNumber = 2.0
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
            WITH t
            CREATE (c:DiscourseCategory {name: 'SampleCat2'})-[:HAS_TOPIC]->(t)
            """
        )

        posts = fetch_raw_posts(
            forum_endpoint=forum_endpoint, from_date=datetime(2020, 1, 1)
        )

        # we inserted 2 posts
        self.assertEqual(len(posts), 2)

        for row in posts:
            data = row.data()

            if data["author_username"] == "user#1":
                self.assertEqual(data["author_name"], "user1")
                self.assertEqual(data["topic"], "topic#1")
                self.assertEqual(data["topic_id"], 1)
                self.assertEqual(data["createdAt"], "2022-01-01T00:00:00.000Z")
                self.assertEqual(data["updatedAt"], "2022-01-01T01:00:00.000Z")
                self.assertEqual(data["authorTrustLevel"], 4)
                self.assertEqual(data["postId"], 100)
                self.assertEqual(data["raw"], "texttexttext of post 1")
                self.assertEqual(data["liker_usernames"], [])
                self.assertEqual(data["liker_names"], [])
                self.assertEqual(data["category"], "SampleCat1")
                self.assertEqual(data["replier_usernames"], ["user#2"])
                self.assertEqual(data["replier_names"], ["user2"])
                self.assertEqual(data["forum_endpoint"], "wwwdwadeswdpoi123")
                self.assertEqual(data["post_number"], 1.0)
            elif data["author_username"] == "user#2":
                self.assertEqual(data["author_name"], "user2")
                self.assertEqual(data["topic"], "topic#2")
                self.assertEqual(data["topic_id"], 2)
                self.assertEqual(data["createdAt"], "2022-01-01T00:01:00.000Z")
                self.assertEqual(data["updatedAt"], "2022-01-01T01:01:00.000Z")
                self.assertEqual(data["raw"], "texttexttext of post 2")
                self.assertEqual(data["postId"], 101)
                self.assertEqual(data["authorTrustLevel"], 1)
                self.assertEqual(data["liker_usernames"], [])
                self.assertEqual(data["liker_names"], [])
                self.assertEqual(data["category"], "SampleCat2")
                self.assertEqual(data["replier_usernames"], [])
                self.assertEqual(data["replier_names"], [])
                self.assertEqual(data["forum_endpoint"], "wwwdwadeswdpoi123")
                self.assertEqual(data["post_number"], 2.0)
            else:
                raise IndexError("It shouldn't get here!")

    def test_fetch_some_data_with_from_date(self):
        neo4j_ops = Neo4jConnection().neo4j_ops
        forum_endpoint = "wwwdwadeswdpoi123"

        neo4j_ops.neo4j_driver.execute_query(
            """
            MATCH (n) DETACH DELETE (n)
            """
        )

        neo4j_ops.neo4j_driver.execute_query(
            """
            CREATE (p:DiscoursePost)
            SET
                p.endpoint = "wwwdwadeswdpoi123",
                p.raw = "texttexttext of post 1",
                p.topicId = 1,
                p.id = 100,
                p.createdAt = '2022-01-01T00:00:00.000Z',
                p.updatedAt = '2022-01-01T01:00:00.000Z',
                p.postNumber = 1.0
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
                p.endpoint = "wwwdwadeswdpoi123",
                p.raw = "texttexttext of post 2",
                p.topicId = 2,
                p.id = 101,
                p.createdAt = '2022-05-01T00:01:00.000Z',
                p.updatedAt = '2022-05-01T01:01:00.000Z',
                p.postNumber = 2.0
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
            WITH t
            CREATE (c:DiscourseCategory {name: 'SampleCat2'})-[:HAS_TOPIC]->(t)
            """
        )

        posts = fetch_raw_posts(
            forum_endpoint=forum_endpoint, from_date=datetime(2022, 3, 1)
        )

        # we should get one of the posts
        self.assertEqual(len(posts), 1)

        for row in posts:
            data = row.data()

            if data["author_username"] == "user#2":
                self.assertEqual(data["author_name"], "user2")
                self.assertEqual(data["topic"], "topic#2")
                self.assertEqual(data["post_number"], 2.0)
                self.assertEqual(data["createdAt"], "2022-05-01T00:01:00.000Z")
                self.assertEqual(data["updatedAt"], "2022-05-01T01:01:00.000Z")
                self.assertEqual(data["raw"], "texttexttext of post 2")
                self.assertEqual(data["authorTrustLevel"], 1)
                self.assertEqual(data["liker_usernames"], [])
                self.assertEqual(data["liker_names"], [])
                self.assertEqual(data["category"], "SampleCat2")
                self.assertEqual(data["replier_usernames"], [])
                self.assertEqual(data["replier_names"], [])
            else:
                raise IndexError("It shouldn't get here!")
