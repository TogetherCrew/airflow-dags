import datetime
import unittest

from analyzer_helper.telegram.utils.is_user_bot import UserBotChecker
from github.neo4j_storage.neo4j_connection import Neo4jConnection


class UserBotCheckerUnit(unittest.TestCase):
    def setUp(self):
        self.neo4jConnection = Neo4jConnection()
        self.driver = self.neo4jConnection.connect_neo4j()
        self.user_bot_checker = UserBotChecker()

        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

        with self.driver.session() as session:
            session.run(
                """
                CREATE 
                    (c:TGChat {title: $chat_title}),
                    (u1:TGUser {id: $id1, is_bot: True, name: 'User One'}),
                    (u2:TGUser {id: $id2, is_bot: False, name: 'User Two'}),
                    (m1:TGMessage {id: '3', text: 'Welcome to the TC Ingestion Pipeline', created_at: $created_at1}),
                    (m2:TGMessage {id: '4', text: 'Hi', created_at: $created_at2}),
                    (m3:TGMessage {id: '5', text: 'Reply', created_at: $created_at3}),
                    (m1)<-[:SENT_IN]-(c),
                    (m2)<-[:SENT_IN]-(c),
                    (m3)<-[:SENT_IN]-(c),
                    (u1)-[:CREATED_MESSAGE]->(m1),
                    (u2)-[:CREATED_MESSAGE]->(m2),
                    (u2)-[:REPLIED]->(m1),
                    (u1)-[:REACTED_TO {new_reaction: '[{"type":"emoji","emoji":"🍓"}]', date: $reaction_date}]->(m1)
                """,
                {
                    "chat_title": "Test Chat",
                    "id1": float("927814807.0"),
                    "id2": float("203678862.0"),
                    "created_at1": int(
                        datetime.datetime(2023, 1, 1).timestamp() * 1000
                    ),
                    "created_at2": int(
                        datetime.datetime(2023, 1, 2).timestamp() * 1000
                    ),
                    "created_at3": int(
                        datetime.datetime(2023, 1, 3).timestamp() * 1000
                    ),
                    "reaction_date": int(
                        datetime.datetime(2023, 1, 4).timestamp() * 1000
                    ),
                },
            )

    def tearDown(self):
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        self.driver.close()

    def test_user_bot_status(self):
        result = self.user_bot_checker.is_user_bot(float("927814807.0"))
        self.assertTrue(result)
        result = self.user_bot_checker.is_user_bot(float("203678862.0"))
        self.assertFalse(result)
