from datetime import datetime

from hivemind_etl_helpers.src.db.telegram.schema import TelegramMessagesModel
from tc_neo4j_lib import Neo4jOps


class ExtractMessages:
    def __init__(self, chat_id: str) -> None:
        self.chat_id = chat_id
        self._connection = Neo4jOps.get_instance()

    def extract(self, from_date: datetime | None = None) -> list[TelegramMessagesModel]:
        """
        extract messages related to the given `chat_id`

        Parameters
        -----------
        from_date : datetime | None
            load from a specific date
            if not given, load all data

        Returns
        ---------
        tg_messages : list[TelegramMessagesModel]
            the telegram messages
        """
        # initialize
        where_clause: str | None = None
        from_date_timestamp: int | None = None

        if from_date:
            from_date_timestamp = int(from_date.timestamp() * 1000)
            where_clause = """
            AND message.date >= $from_date_timestamp
            """
        query = f"""
            MATCH (c:TGChat {{id: $chat_id}})<-[:SENT_IN]-(message:TGMessage)
            WHERE message.text IS NOT NULL
            {where_clause if where_clause else ""}
            WITH
                message.id AS message_id,
                MAX(message.updated_at) AS latest_msg_time,
                MIN(message.updated_at) AS first_msg_time

            MATCH (first_message:TGMessage {{id: message_id, updated_at: first_msg_time}})
            MATCH (last_edit:TGMessage {{id: message_id, updated_at: latest_msg_time}})

            WITH
                first_message AS message,
                last_edit.updated_at AS edited_at,
                last_edit.text AS message_text
            MATCH (author:TGUser)-[created_rel:CREATED_MESSAGE]->(message)
            WHERE NOT EXISTS {{
                MATCH (author)-[banned_rel:BANNED]->(c:TGChat {{id: $chat_id}})
                MATCH (author)-[joined_rel:JOINED|UNBANNED]->(c)
                WITH author, MAX(banned_rel.date) AS banned_time, MAX(joined_rel.date) AS joined_time
                WHERE banned_time > joined_time
            }}
            OPTIONAL MATCH (reacted_user:TGUser)-[react_rel:REACTED_TO]->(message)
            OPTIONAL MATCH (reply_msg:TGMessage)-[:REPLIED]->(message)
            OPTIONAL MATCH (replied_user:TGUser)-[:CREATED_MESSAGE]->(reply_msg)
            OPTIONAL MATCH (message)-[:MENTIONED]->(mentioned_user:TGUser)
            RETURN
                message.id AS message_id,
                message_text,
                author.username AS author_username,
                message.date AS message_created_at,
                edited_at AS message_edited_at,
                COLLECT(DISTINCT mentioned_user.username) AS mentions,
                COLLECT(DISTINCT replied_user.username) AS repliers,
                COLLECT(DISTINCT reacted_user.username) AS reactors
            ORDER BY message_created_at DESC
        """

        parameters = {"chat_id": self.chat_id}
        if from_date_timestamp:
            parameters["from_date_timestamp"] = from_date_timestamp

        tg_messages = []
        with self._connection.neo4j_driver.session() as session:
            result = session.run(
                query,
                parameters=parameters,
            )
            messages = result.data()
            tg_messages = [TelegramMessagesModel(**message) for message in messages]

        return tg_messages
