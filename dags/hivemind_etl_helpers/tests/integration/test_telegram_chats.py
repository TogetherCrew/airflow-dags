from unittest import TestCase

from dotenv import load_dotenv
from hivemind_etl_helpers.src.db.telegram.extract import TelegramChats

class TestTelegramChats(TestCase):
    def setUp(self) -> None:
        load_dotenv()
        self.tc_chats = TelegramChats()
        self._delete_everything()

    def tearDown(self) -> None:
        self._delete_everything()
    
    def _delete_everything(self):
        """remove everything on neo4j db"""
        with self.tc_chats._connection.neo4j_driver.session() as session:
            session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE (n)"))
    
    def test_extract_chats_empty_db(self):
        chat_ids = self.tc_chats.extract_chats()
        self.assertEqual(
            chat_ids, [],
            msg="No chat id should be available in case no data available!"
        )

    def test_extract_chats_single_chat(self):
        neo4j_driver = self.tc_chats._connection.neo4j_driver
        with neo4j_driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE (:TGChat {
                            id: $id,
                            title: $title,
                            created_at: $created_at,
                            updated_at: $updated_at,
                            type: $type
                        }
                    )
                    """,
                    parameters={
                        "id": 100000,
                        "title": "test chat",
                        "type": "supergroup",
                        "created_at": 1724224885211,
                        "updated_at": 1728985245911,
                    }
                )
            )
        
        chat_ids = self.tc_chats.extract_chats()
        self.assertEqual(chat_ids, ["100000"])

    def test_extract_chats_multiple_chats(self):
        neo4j_driver = self.tc_chats._connection.neo4j_driver
        with neo4j_driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE (:TGChat {
                            id: $id,
                            title: $title,
                            created_at: $created_at,
                            updated_at: $updated_at,
                            type: $type
                        }
                    )
                    CREATE (:TGChat {
                            id: $id2,
                            title: $title2,
                            created_at: $created_at2,
                            updated_at: $updated_at2,
                            type: $type2
                        }
                    )
                    CREATE (:TGChat {
                            id: $id3,
                            title: $title3,
                            created_at: $created_at3,
                            updated_at: $updated_at3,
                            type: $type3
                        }
                    )
                    """,
                    parameters={
                        "id": 100001,
                        "title": "test chat",
                        "type": "supergroup",
                        "created_at": 1724224885211,
                        "updated_at": 1728985245911,
                        "id2": 100002,
                        "title2": "test chat 2",
                        "type2": "supergroup",
                        "created_at2": 1724224885212,
                        "updated_at2": 1728985245912,
                        "id3": 100003,
                        "title3": "test chat 3",
                        "type3": "supergroup",
                        "created_at3": 1724224885213,
                        "updated_at3": 1728985245913,
                    }
                )
            )
        
        chat_ids = self.tc_chats.extract_chats()
        self.assertEqual(chat_ids, ["100001", "100002", "100003"])
