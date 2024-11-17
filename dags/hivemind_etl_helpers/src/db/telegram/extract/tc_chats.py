import logging

from tc_neo4j_lib import Neo4jOps


class TelegramChats:
    def __init__(self) -> None:
        self._connection = Neo4jOps.get_instance()

    def extract_chats(self) -> list[tuple[int, str]]:
        """
        extract the chat id and chat names

        Returns
        ---------
        chat_info : list[tuple[int, str]]
            a list of Telegram chat id and chat name
        """
        driver = self._connection.neo4j_driver

        chat_info: list[str] = []
        try:
            with driver.session() as session:
                records = session.run(
                    "MATCH (c:TGChat) RETURN c.id as chat_id, c.title as name"
                )
                chat_info = [(record["chat_id"], record["name"]) for record in records]
        except Exception as exp:
            logging.error(f"Exception during extracting chat ids. exp: {exp}")

        return chat_info
