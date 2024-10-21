from unittest import TestCase
from datetime import datetime

from dotenv import load_dotenv
from hivemind_etl_helpers.src.db.telegram.extract import ExtractMessages
from hivemind_etl_helpers.src.db.telegram.schema import TelegramMessagesModel


class TestExtractTelegramMessages(TestCase):
    def setUp(self) -> None:
        load_dotenv()
        self.chat_id = "1234567890"
        self.extractor = ExtractMessages(self.chat_id)
        self._delete_everything()

    def tearDown(self) -> None:
        self._delete_everything()

    def _delete_everything(self):
        """remove everything on neo4j db"""
        with self.extractor._connection.neo4j_driver.session() as session:
            session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE (n)"))

    def test_extract_empty_data(self):
        data = self.extractor.extract()

        self.assertEqual(data, [])

    def test_extract_empty_data_with_from_date(self):
        data = self.extractor.extract(from_date=datetime(2023, 1, 1))

        self.assertEqual(data, [])

    def test_extract_single_data(self):
        with self.extractor._connection.neo4j_driver.session() as session:
            session.run(
                """
                CREATE (c:TGChat {id: $chat_id}),
                    (u1:TGUser {id: '927814807.0', username: 'User One'}),
                    (u2:TGUser {id: '203678862.0', username: 'User Two'}),
                    (m1:TGMessage {
                            id: '3.0',
                            text: '🎉️️️️️️ Welcome to the TC Ingestion Pipeline',
                            date: $created_at1,
                            updated_at: $created_at1
                        }
                    ),
                    (m1)-[:SENT_IN]->(c),
                    (u1)-[:CREATED_MESSAGE]->(m1),
                    (u2)-[:REACTED_TO {new_reaction: '[{"type":"emoji","emoji":"🍓"}]', date: $reaction_date}]->(m1)
                """,
                {
                    "chat_id": self.chat_id,
                    "created_at1": 1672531200.0,  # Sunday, January 1, 2023 12:00:00 AM
                    "reaction_date": 1672790400.0,  # Wednesday, January 4, 2023 12:00:00 AM
                },
            )
        data = self.extractor.extract()

        self.assertEqual(
            data,
            [
                TelegramMessagesModel(
                    message_id=3,
                    message_text="🎉️️️️️️ Welcome to the TC Ingestion Pipeline",
                    author_username="User One",
                    message_created_at=1672531200,
                    message_edited_at=1672531200,
                    mentions=[],
                    repliers=[],
                    reactors=["User Two"],
                )
            ],
        )

    def test_extract_single_data_with_from_date(self):
        with self.extractor._connection.neo4j_driver.session() as session:
            session.run(
                """
                CREATE (c:TGChat {id: $chat_id}),
                    (u1:TGUser {id: '927814807.0', username: 'User One'}),
                    (u2:TGUser {id: '203678862.0', username: 'User Two'}),
                    (m1:TGMessage {
                            id: '3.0',
                            text: '🎉️️️️️️ Welcome to the TC Ingestion Pipeline',
                            date: $created_at1,
                            updated_at: $created_at1
                        }
                    ),
                    (m1)-[:SENT_IN]->(c),
                    (u1)-[:CREATED_MESSAGE]->(m1),
                    (u2)-[:REACTED_TO {new_reaction: '[{"type":"emoji","emoji":"🍓"}]', date: $reaction_date}]->(m1)
                """,
                {
                    "chat_id": self.chat_id,
                    "created_at1": 1672531200.0,  # Sunday, January 1, 2023 12:00:00 AM
                    "reaction_date": 1672790400.0,  # Wednesday, January 4, 2023 12:00:00 AM
                },
            )
        data = self.extractor.extract(from_date=datetime(2024, 1, 1))

        self.assertEqual(data, [])

    def test_extract_multiple_data(self):
        with self.extractor._connection.neo4j_driver.session() as session:
            session.run(
                """
                CREATE (c:TGChat {id: $chat_id}),
                    (u1:TGUser {id: '927814807.0', username: 'User One'}),
                    (u2:TGUser {id: '203678862.0', username: 'User Two'}),
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
                    (u2)-[:REACTED_TO {new_reaction: '[{"type":"emoji","emoji":"🍓"}]', date: $reaction_date}]->(m1)
                """,
                {
                    "chat_id": self.chat_id,
                    "created_at1": 1672531200.0,  # Sunday, January 1, 2023 12:00:00 AM
                    "created_at4": 1672531205.0,  # Sunday, January 1, 2023 12:00:05 AM
                    "created_at2": 1672617600.0,  # Monday, January 2, 2023 12:00:00 AM
                    "created_at3": 1672704000.0,  # Tuesday, January 3, 2023 12:00:00 AM
                    "reaction_date": 1672790400.0,  # Wednesday, January 4, 2023 12:00:00 AM
                },
            )
        data = self.extractor.extract()
        print("data", data)

        expected_data = [
            TelegramMessagesModel(
                message_id=3,
                message_text="🎉️️️️️️ Welcome to the TC Ingestion Pipeline. EDITED MSG",
                author_username="User One",
                message_created_at=1672531200.0,
                message_edited_at=1672531205.0,
                mentions=[],
                repliers=["User Two"],
                reactors=["User Two"],
            ),
            TelegramMessagesModel(
                message_id=4,
                message_text="Hi",
                author_username="User Two",
                message_created_at=1672617600.0,
                message_edited_at=1672617600.0,
                mentions=[],
                repliers=[],
                reactors=[],
            ),
            TelegramMessagesModel(
                message_id=5,
                message_text="Reply🫡",
                author_username="User Two",
                message_created_at=1672704000.0,
                message_edited_at=1672704000.0,
                mentions=[],
                repliers=[],
                reactors=[],
            ),
        ]

        self.assertEqual(len(data), 3)
        for d in data:
            self.assertIn(d, expected_data)

    def test_extract_multiple_data_with_from_date(self):
        with self.extractor._connection.neo4j_driver.session() as session:
            session.run(
                """
                CREATE (c:TGChat {id: $chat_id}),
                    (u1:TGUser {id: '927814807.0', username: 'User One'}),
                    (u2:TGUser {id: '203678862.0', username: 'User Two'}),
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
                    (u2)-[:REACTED_TO {new_reaction: '[{"type":"emoji","emoji":"🍓"}]', date: $reaction_date}]->(m1)
                """,
                {
                    "chat_id": self.chat_id,
                    "created_at1": 1672531200.0,  # Sunday, January 1, 2023 12:00:00 AM
                    "created_at4": 1672531205.0,  # Sunday, January 1, 2023 12:00:05 AM
                    "created_at2": 1672617600.0,  # Monday, January 2, 2023 12:00:00 AM
                    "created_at3": 1672704000.0,  # Tuesday, January 3, 2023 12:00:00 AM
                    "reaction_date": 1672790400.0,  # Wednesday, January 4, 2023 12:00:00 AM
                },
            )
        data = self.extractor.extract(from_date=datetime(2024, 1, 1))

        self.assertEqual(data, [])
