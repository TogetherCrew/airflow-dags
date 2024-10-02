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
        cls.chat_id = "test_group"
        cls.platform_id = "platform_db"
        cls.platform_db = cls.client[cls.platform_id]
        cls.extractor = ExtractRawInfo(cls.chat_id, cls.platform_id)
        cls.rawmemberactivities_collection = cls.platform_db["rawmemberactivities"]

        with cls.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

        with cls.driver.session() as session:
            session.run(
                """
                CREATE (c:TGChat {id: $chat_id}),
                    (u1:TGUser {id: '927814807.0', name: 'User One'}),
                    (u2:TGUser {id: '203678862.0', name: 'User Two'}),
                    (m1:TGMessage {
                            id: '3.0',
                            text: '🎉️️️️️️ Welcome to the TC Ingestion Pipeline',
                            date: $created_at1,
                            updated_at: $created_at1
                        }
                    ),
                    (m4:TGMessage {
                            id: '3.0',
                            text: '🎉️️️️️️ Welcome to the TC Ingestion Pipeline. EDITED MSG',
                            date: $created_at4,
                            updated_at: $created_at4
                        }
                    ),
                    (m2:TGMessage {
                            id: '4.0',
                            text: 'Hi',
                            date: $created_at2,
                            updated_at: $created_at2
                        }
                    ),
                    (m3:TGMessage {
                            id: '5.0',
                            text: 'Reply🫡',
                            date: $created_at3,
                            updated_at: $created_at3
                        }
                    ),
                    (m1)-[:SENT_IN]->(c),
                    (m2)-[:SENT_IN]->(c),
                    (m3)-[:SENT_IN]->(c),
                    (m4)-[:SENT_IN]->(c),
                    (u1)-[:CREATED_MESSAGE]->(m1),
                    (u2)-[:CREATED_MESSAGE]->(m2),
                    (u2)-[:CREATED_MESSAGE]->(m3),
                    (m1)-[:EDITED]->(m4),
                    (m3)-[:REPLIED]->(m1),
                    (u1)-[:REACTED_TO {new_reaction: '[{"type":"emoji","emoji":"🍓"}]', date: $reaction_date}]->(m1)
                """,
                {
                    "chat_id": cls.chat_id,
                    "created_at1": 1672531200.0,
                    "created_at4": 1672531205.0,
                    "created_at2": 1672617600.0,
                    "created_at3": 1672704000.0,
                    "reaction_date": 1672790400.0,
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
                "message_id": "3.0",
                "message_text": "🎉️️️️️️ Welcome to the TC Ingestion Pipeline. EDITED MSG",
                "message_created_at": 1672531200.0,
                "message_edited_at": 1672531205.0,
                "author_id": "927814807.0",
                "reactions": [
                    {
                        "reaction": '[{"type":"emoji","emoji":"🍓"}]',
                        "reaction_date": 1672790400.0,
                        "reactor_id": "927814807.0",
                    }
                ],
                "replies": [
                    {
                        "replied_date": 1672704000.0,
                        "replier_id": "203678862.0",
                        "reply_message_id": "5.0",
                    }
                ],
                "mentions": [],
            },
            {
                "message_id": "4.0",
                "message_text": "Hi",
                "message_created_at": 1672617600.0,
                "message_edited_at": 1672617600.0,
                "author_id": "203678862.0",
                "reactions": [],
                "replies": [],
                "mentions": [],
            },
            {
                "message_id": "5.0",
                "message_text": "Reply🫡",
                "message_created_at": 1672704000.0,
                "message_edited_at": 1672704000.0,
                "author_id": "203678862.0",
                "reactions": [],
                "replies": [],
                "mentions": [],
            },
        ]
        self.assertEqual(len(result), 3)
        for res in result:
            self.assertIn(res, expected_result)

    def test_fetch_raw_data(self):
        result = self.extractor.fetch_raw_data()
        expected_result = [
            {
                "message_id": "3.0",
                "message_text": "🎉️️️️️️ Welcome to the TC Ingestion Pipeline. EDITED MSG",
                "message_created_at": 1672531200.0,
                "message_edited_at": 1672531205.0,
                "author_id": "927814807.0",
                "reactions": [
                    {
                        "reaction": '[{"type":"emoji","emoji":"🍓"}]',
                        "reaction_date": 1672790400.0,
                        "reactor_id": "927814807.0",
                    }
                ],
                "replies": [
                    {
                        "replied_date": 1672704000.0,
                        "replier_id": "203678862.0",
                        "reply_message_id": "5.0",
                    }
                ],
                "mentions": [],
            },
            {
                "message_id": "4.0",
                "message_text": "Hi",
                "message_created_at": 1672617600.0,
                "message_edited_at": 1672617600.0,
                "author_id": "203678862.0",
                "reactions": [],
                "replies": [],
                "mentions": [],
            },
            {
                "message_id": "5.0",
                "message_text": "Reply🫡",
                "message_created_at": 1672704000.0,
                "message_edited_at": 1672704000.0,
                "author_id": "203678862.0",
                "reactions": [],
                "replies": [],
                "mentions": [],
            },
        ]
        self.assertEqual(len(result), 3)
        for res in result:
            print(res)
            self.assertIn(res, expected_result)

    def test_extract_with_recompute(self):
        self.rawmemberactivities_collection.delete_many({})

        period = datetime.datetime.now()
        result = self.extractor.extract(period, recompute=True)

        expected_result = [
            {
                "message_id": "3.0",
                "message_text": "🎉️️️️️️ Welcome to the TC Ingestion Pipeline. EDITED MSG",
                "message_created_at": 1672531200.0,
                "message_edited_at": 1672531205.0,
                "author_id": "927814807.0",
                "reactions": [
                    {
                        "reaction": '[{"type":"emoji","emoji":"🍓"}]',
                        "reaction_date": 1672790400.0,
                        "reactor_id": "927814807.0",
                    }
                ],
                "replies": [
                    {
                        "replied_date": 1672704000.0,
                        "replier_id": "203678862.0",
                        "reply_message_id": "5.0",
                    }
                ],
                "mentions": [],
            },
            {
                "message_id": "4.0",
                "message_text": "Hi",
                "message_created_at": 1672617600.0,
                "message_edited_at": 1672617600.0,
                "author_id": "203678862.0",
                "reactions": [],
                "replies": [],
                "mentions": [],
            },
            {
                "message_id": "5.0",
                "message_text": "Reply🫡",
                "message_created_at": 1672704000.0,
                "message_edited_at": 1672704000.0,
                "author_id": "203678862.0",
                "reactions": [],
                "replies": [],
                "mentions": [],
            },
        ]
        for res in result:
            self.assertIn(res, expected_result)

    def test_extract_without_recompute_no_latest_activity(self):
        self.rawmemberactivities_collection.delete_many({})

        result = self.extractor.extract(
            period=datetime.datetime(2023, 1, 1), recompute=False
        )
        expected_result = [
            {
                "message_id": "3.0",
                "message_text": "🎉️️️️️️ Welcome to the TC Ingestion Pipeline. EDITED MSG",
                "message_created_at": 1672531200.0,
                "message_edited_at": 1672531205.0,
                "author_id": "927814807.0",
                "reactions": [
                    {
                        "reaction": '[{"type":"emoji","emoji":"🍓"}]',
                        "reaction_date": 1672790400.0,
                        "reactor_id": "927814807.0",
                    }
                ],
                "replies": [
                    {
                        "replied_date": 1672704000.0,
                        "replier_id": "203678862.0",
                        "reply_message_id": "5.0",
                    }
                ],
                "mentions": [],
            },
            {
                "message_id": "4.0",
                "message_text": "Hi",
                "message_created_at": 1672617600.0,
                "message_edited_at": 1672617600.0,
                "author_id": "203678862.0",
                "reactions": [],
                "replies": [],
                "mentions": [],
            },
            {
                "message_id": "5.0",
                "message_text": "Reply🫡",
                "message_created_at": 1672704000.0,
                "message_edited_at": 1672704000.0,
                "author_id": "203678862.0",
                "reactions": [],
                "replies": [],
                "mentions": [],
            },
        ]
        self.assertEqual(len(result), 3)
        for res in result:
            self.assertIn(res, expected_result)

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
        inserted_data = list(self.rawmemberactivities_collection.find())

        result = self.extractor.extract(
            period=datetime.datetime(2023, 1, 1), recompute=False
        )
        expected_result = [
            {
                "message_id": "3.0",
                "message_text": "🎉️️️️️️ Welcome to the TC Ingestion Pipeline. EDITED MSG",
                "message_created_at": 1672531200.0,
                "message_edited_at": 1672531205.0,
                "author_id": "927814807.0",
                "reactions": [
                    {
                        "reaction": '[{"type":"emoji","emoji":"🍓"}]',
                        "reaction_date": 1672790400.0,
                        "reactor_id": "927814807.0",
                    }
                ],
                "replies": [
                    {
                        "reply_message_id": "5.0",
                        "replier_id": "203678862.0",
                        "replied_date": 1672704000.0,
                    }
                ],
                "mentions": [],
            },
            {
                "message_id": "4.0",
                "message_text": "Hi",
                "message_created_at": 1672617600.0,
                "message_edited_at": 1672617600.0,
                "author_id": "203678862.0",
                "reactions": [],
                "replies": [],
                "mentions": [],
            },
            {
                "message_id": "5.0",
                "message_text": "Reply🫡",
                "message_created_at": 1672704000.0,
                "message_edited_at": 1672704000.0,
                "author_id": "203678862.0",
                "reactions": [],
                "replies": [],
                "mentions": [],
            },
        ]
        self.assertEqual(len(result), 3)
        for res in result:
            self.assertIn(res, expected_result)

    def test_extract_without_recompute_latest_activity_after_period(self):
        self.rawmemberactivities_collection.delete_many({})
        self.rawmemberactivities_collection.insert_one(
            {
                "author_id": "203678862.0",
                "date": datetime.datetime(
                    2023, 1, 2, 00, 00, 00, tzinfo=datetime.timezone.utc
                ),
                "source_id": "4.0",
                "metadata": {
                    "chat_id": 6134,
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
                "message_id": "5.0",
                "message_text": "Reply🫡",
                "message_created_at": 1672704000.0,
                "message_edited_at": 1672704000.0,
                "author_id": "203678862.0",
                "reactions": [],
                "replies": [],
                "mentions": [],
            },
        ]
        self.assertEqual(len(result), 1)
        self.assertEqual(result, expected_result)
