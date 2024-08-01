import datetime
import unittest

from analyzer_helper.telegram.extract_raw_data import ExtractRawInfo
from github.neo4j_storage.neo4j_connection import Neo4jConnection
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


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
            CREATE (c:TGChat {title: $chat_title}),
                (u1:TGUser {id: '927814807', name: 'User One'}),
                (u2:TGUser {id: '203678862', name: 'User Two'}),
                (m1:TGMessage {id: '3', text: '🎉️️️️️️ Welcome to the TC Ingestion Pipeline', created_at: $created_at1}),
                (m2:TGMessage {id: '4', text: 'Hi', created_at: $created_at2}),
                (m3:TGMessage {id: '5', text: 'Reply🫡', created_at: $created_at3}),
                (m1)<-[:SENT_IN]-(c),
                (m2)<-[:SENT_IN]-(c),
                (m3)<-[:SENT_IN]-(c),
                (u1)-[:CREATED_MESSAGE]->(m1),
                (u2)-[:CREATED_MESSAGE]->(m2),
                (u2)-[:REPLIED]->(m1),
                (u1)-[:REACTED_TO {new_reaction: '[{"type":"emoji","emoji":"🍓"}]', date: $reaction_date}]->(m1)
            """,
                {
                    "chat_title": cls.forum_endpoint,
                    "created_at1": int(datetime.datetime(2023, 1, 1, 0, 0).timestamp() * 1000),
                    "created_at2": int(datetime.datetime(2023, 1, 2, 0, 0).timestamp() * 1000),
                    "created_at3": int(datetime.datetime(2023, 1, 3, 0, 0).timestamp() * 1000),
                    "reaction_date": int(datetime.datetime(2023, 1, 4, 0, 0).timestamp() * 1000),
                },
            )

    @classmethod
    def tearDownClass(cls):
        with cls.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        cls.extractor.close()
        cls.driver.close()

    def test_fetch_message_details(self):
        result = self.extractor.fetch_message_details()
        expected_result = [
            {
                "message_id": 3.0,
                "message_text": "🎉️️️️️️ Welcome to the TC Ingestion Pipeline",
                "user_id": "927814807",
                "created_date": None,
                "reactions": [
                    {
                        "reactor_id": "203678862",
                        "reaction": '[{"type":"emoji","emoji":"🍓"}]',
                        "reaction_date": 1672531200000,
                    }
                ],
                "replies": [{"reply_message_id": "4", "replier_id": "203678862", "replied_date": None}],
                "mentions": [],
            },
            {
                "message_id": 4.0,
                "message_text": "Hi",
                "user_id": "203678862",
                "created_date": None,
                "reactions": [],
                "replies": [],
                "mentions": [],
            },
            {
                "message_id": 5.0,
                "message_text": "Reply🫡",
                "user_id": "203678862",
                "created_date": None,
                "reactions": [],
                "replies": [],
                "mentions": [],
            }
        ]
        self.assertEqual(len(result), 3)
        self.assertEqual(result, expected_result)

    def test_fetch_raw_data(self):
        result = self.extractor.fetch_raw_data()
        expected_result = [
            {
                "message_id": 3.0,
                "message_text": "🎉️️️️️️ Welcome to the TC Ingestion Pipeline",
                "user_id": "927814807",
                "created_date": None,
                "reactions": [
                    {
                        "reactor_id": "203678862",
                        "reaction": '[{"type":"emoji","emoji":"🍓"}]',
                        "reaction_date": 1672531200000,
                    }
                ],
                "replies": [{"reply_message_id": "4", "replier_id": "203678862", "replied_date": None}],
                "mentions": [],
            },
            {
                "message_id": 4.0,
                "message_text": "Hi",
                "user_id": "203678862",
                "created_date": None,
                "reactions": [],
                "replies": [],
                "mentions": [],
            },
            {
                "message_id": 5.0,
                "message_text": "Reply🫡",
                "user_id": "203678862",
                "created_date": None,
                "reactions": [],
                "replies": [],
                "mentions": [],
            }
        ]
        self.assertEqual(len(result), 3)
        self.assertEqual(result, expected_result)

    def test_extract_with_recompute(self):
        self.rawmemberactivities_collection.delete_many({})

        period = datetime.datetime.now()
        data = self.extractor.extract(period, recompute=True)

        # Check if data is fetched from the Neo4j database without date filtering
        self.assertEqual(len(data), 3)
        author_ids = [post["user_id"] for post in data]
        self.assertIn("927814807", author_ids)
        self.assertIn("203678862", author_ids)

    def test_extract_without_recompute_no_latest_activity(self):
        self.rawmemberactivities_collection.delete_many({})

        result = self.extractor.extract(
            period=datetime.datetime(2023, 1, 1), recompute=False
        )
        expected_result = [
            {
                "message_id": 3.0,
                "message_text": "🎉️️️️️️ Welcome to the TC Ingestion Pipeline",
                "user_id": "927814807",
                "created_date": None,
                "reactions": [
                    {
                        "reactor_id": "203678862",
                        "reaction": '[{"type":"emoji","emoji":"🍓"}]',
                        "reaction_date": 1672531200000,
                    }
                ],
                "replies": [{"reply_message_id": "4", "replier_id": "203678862", "replied_date": None}],
                "mentions": [],
            },
            {
                "message_id": 4.0,
                "message_text": "Hi",
                "user_id": "203678862",
                "created_date": None,
                "reactions": [],
                "replies": [],
                "mentions": [],
            },
            {
                "message_id": 5.0,
                "message_text": "Reply🫡",
                "user_id": "203678862",
                "created_date": None,
                "reactions": [],
                "replies": [],
                "mentions": [],
            }
        ]
        self.assertEqual(len(result), 3)
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
                "message_id": 3.0,
                "message_text": "🎉️️️️️️ Welcome to the TC Ingestion Pipeline",
                "user_id": "927814807",
                "created_date": None,
                "reactions": [
                    {
                        "reactor_id": "203678862",
                        "reaction": '[{"type":"emoji","emoji":"🍓"}]',
                        "reaction_date": 1672531200000,
                    }
                ],
                "replies": [{"reply_message_id": "4", "replier_id": "203678862", "replied_date": None}],
                "mentions": [],
            },
            {
                "message_id": 4.0,
                "message_text": "Hi",
                "user_id": "203678862",
                "created_date": None,
                "reactions": [],
                "replies": [],
                "mentions": [],
            },
            {
                "message_id": 5.0,
                "message_text": "Reply🫡",
                "user_id": "203678862",
                "created_date": None,
                "reactions": [],
                "replies": [],
                "mentions": [],
            }
        ]
        self.assertEqual(len(result), 3)
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
