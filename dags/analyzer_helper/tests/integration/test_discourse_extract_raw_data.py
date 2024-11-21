import datetime
import unittest

from analyzer_helper.discourse.extract_raw_data import ExtractRawInfo
from github.neo4j_storage.neo4j_connection import Neo4jConnection
from tc_hivemind_backend.db.mongo import MongoSingleton


class TestExtractRawInfo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.neo4jConnection = Neo4jConnection()
        cls.client = MongoSingleton.get_instance().client
        cls.driver = cls.neo4jConnection.connect_neo4j()
        cls.forum_endpoint = "http://test_forum"
        cls.platform_id = "platform_db"
        cls.platform_db = cls.client[cls.platform_id]
        cls.extractor = ExtractRawInfo(cls.forum_endpoint, cls.platform_id)
        cls.rawmemberactivities_collection = cls.platform_db["rawmemberactivities"]

        with cls.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

        with cls.driver.session() as session:
            session.run(
                """
            CREATE (u1:DiscourseUser {id: 'user1', name: 'User One'}),
                (u2:DiscourseUser {id: 'user2', name: 'User Two'}),
                (p1:DiscoursePost 
                    {
                        id: '1',
                        content: 'Post 1',
                        createdAt: '2023-01-01T00:00:00Z',
                        topicId: 'topic-uuid',
                        endpoint: 'http://test_forum',
                        raw: "Sample Text 1",
                        postNumber: 1.0
                    }
                ),
                (p2:DiscoursePost 
                    {
                        id: '2',
                        content: 'Post 2',
                        createdAt: '2023-01-02T00:00:00Z',
                        topicId: 'topic-uuid',
                        endpoint: 'http://test_forum',
                        raw: "Sample Text 2",
                        postNumber: 2.0
                    }
                ),
                (t:DiscourseTopic {id: 'topic-uuid', endpoint: 'http://test_forum'}),
                (c:DiscourseCategory {id: 'category1', name: 'Category 1'}),
                (p1)<-[:HAS_POST]-(t),
                (p2)<-[:HAS_POST]-(t),
                (p1)<-[:POSTED]-(u1),
                (p2)<-[:POSTED]-(u2),
                (p1)<-[:LIKED]-(u2),
                (p2)<-[:REPLIED_TO]-(p1),
                (c)-[:HAS_TOPIC]->(t)
            """,
                {"endpoint": cls.forum_endpoint},
            )

    @classmethod
    def tearDownClass(cls):
        with cls.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        cls.extractor.close()
        cls.driver.close()

    def test_fetch_post_details(self):
        result = self.extractor.fetch_post_details()
        expected_result = [
            {
                "post_id": "1",
                "author_id": "user1",
                "created_at": "2023-01-01T00:00:00Z",
                "reactions": ["user2"],
                "replied_post_id": "2",
                "replied_post_user_id": "user2",
                "topic_id": "topic-uuid",
                "post_number": 1.0,
                "text": "Sample Text 1",
            },
            {
                "post_id": "2",
                "author_id": "user2",
                "created_at": "2023-01-02T00:00:00Z",
                "reactions": [],
                "replied_post_id": None,
                "replied_post_user_id": None,
                "topic_id": "topic-uuid",
                "post_number": 2.0,
                "text": "Sample Text 2",
            },
        ]
        self.assertEqual(len(result), 2)
        self.assertEqual(result, expected_result)

    def test_fetch_post_categories(self):
        post_details = self.extractor.fetch_post_details()
        post_ids = [post["post_id"] for post in post_details]
        post_categories = self.extractor.fetch_post_categories(post_ids)
        self.assertEqual(len(post_categories), 2)

        for category in post_categories:
            self.assertIn("post_id", category)
            self.assertIn("category_id", category)

    def test_fetch_raw_data(self):
        combined_data = self.extractor.fetch_raw_data()
        self.assertEqual(len(combined_data), 2)
        category_ids = [post["category_id"] for post in combined_data]
        self.assertIn("category1", category_ids)

        for post in combined_data:
            self.assertIn("post_id", post)
            self.assertIn("author_id", post)
            self.assertIn("created_at", post)
            self.assertIn("reactions", post)
            self.assertIn("replied_post_id", post)
            self.assertIn("topic_id", post)
            self.assertIn("category_id", post)

    def test_extract_with_recompute(self):
        self.rawmemberactivities_collection.delete_many({})

        period = datetime.datetime.now()
        data = self.extractor.extract(period, recompute=True)

        # Check if data is fetched from the Neo4j database without date filtering
        self.assertEqual(len(data), 2)
        author_ids = [post["author_id"] for post in data]
        self.assertIn("user1", author_ids)
        self.assertIn("user2", author_ids)

    def test_extract_without_recompute_no_latest_activity(self):
        self.rawmemberactivities_collection.delete_many({})

        result = self.extractor.extract(
            period=datetime.datetime(2023, 1, 1), recompute=False
        )
        expected_result = [
            {
                "post_id": "1",
                "author_id": "user1",
                "created_at": "2023-01-01T00:00:00Z",
                "reactions": ["user2"],
                "replied_post_id": "2",
                "replied_post_user_id": "user2",
                "topic_id": "topic-uuid",
                "category_id": "category1",
                "post_number": 1.0,
                "text": "Sample Text 1",
            },
            {
                "post_id": "2",
                "author_id": "user2",
                "created_at": "2023-01-02T00:00:00Z",
                "reactions": [],
                "replied_post_id": None,
                "replied_post_user_id": None,
                "topic_id": "topic-uuid",
                "category_id": "category1",
                "post_number": 2.0,
                "text": "Sample Text 2",
            },
        ]
        self.assertEqual(len(result), 2)
        self.assertEqual(result, expected_result)

    def test_extract_without_recompute_latest_activity_before_period(self):
        self.rawmemberactivities_collection.delete_many({})
        self.rawmemberactivities_collection.insert_one(
            {
                "author_id": "6168",
                "date": datetime.datetime(
                    2022, 12, 31, 00, 00, 00, tzinfo=datetime.timezone.utc
                ),
                "source_id": "6262",
                "metadata": {
                    "category_id": None,
                    "topic_id": 6134,
                    "bot_activity": False,
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
                "interactions": [
                    {
                        "name": "reply",
                        "type": "emitter",
                        "users_engaged_id": ["4444"],
                    }
                ],
            },
        )

        result = self.extractor.extract(
            period=datetime.datetime(2023, 1, 1), recompute=False
        )
        expected_result = [
            {
                "post_id": "1",
                "author_id": "user1",
                "created_at": "2023-01-01T00:00:00Z",
                "reactions": ["user2"],
                "replied_post_id": "2",
                "replied_post_user_id": "user2",
                "topic_id": "topic-uuid",
                "category_id": "category1",
                "post_number": 1.0,
                "text": "Sample Text 1",
            },
            {
                "post_id": "2",
                "author_id": "user2",
                "created_at": "2023-01-02T00:00:00Z",
                "reactions": [],
                "replied_post_id": None,
                "replied_post_user_id": None,
                "topic_id": "topic-uuid",
                "category_id": "category1",
                "post_number": 2.0,
                "text": "Sample Text 2",
            },
        ]
        self.assertEqual(len(result), 2)
        self.assertEqual(result, expected_result)

    def test_extract_without_recompute_latest_activity_after_period(self):
        self.rawmemberactivities_collection.delete_many({})
        self.rawmemberactivities_collection.insert_one(
            {
                "author_id": "6168",
                "date": datetime.datetime(
                    2023, 1, 2, 00, 00, 00, tzinfo=datetime.timezone.utc
                ),
                "source_id": "6262",
                "metadata": {
                    "category_id": None,
                    "topic_id": 6134,
                    "bot_activity": False,
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
                "interactions": [
                    {
                        "name": "reply",
                        "type": "emitter",
                        "users_engaged_id": ["4444"],
                    }
                ],
            },
        )

        result = self.extractor.extract(
            period=datetime.datetime(2023, 1, 1), recompute=False
        )
        expected_result = []
        self.assertEqual(result, expected_result)
