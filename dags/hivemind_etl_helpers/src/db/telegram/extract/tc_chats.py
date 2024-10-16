import logging

from tc_neo4j_lib import Neo4jOps


class TelegramChats:
    def __init__(self) -> None:
        self._connection = Neo4jOps.get_instance()

    def extract_chat_ids(self) -> list[str]:
        """
        extract the chat ids

        Returns
        ---------
        chat_ids : list[str]
            a list of Telegram chat id
        """
        driver = self._connection.neo4j_driver

        chat_ids: list[str] = []
        try:
            with driver.session() as session:
                records = session.run("MATCH (c:TGChat) RETURN c.id as chat_id")
                chat_ids = [str(record["chat_id"]) for record in records]
        except Exception as exp:
            logging.error(f"Exception during extracting chat ids. exp: {exp}")

        return chat_ids
