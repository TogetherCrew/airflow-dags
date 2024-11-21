from datetime import datetime
from typing import Any, Dict, List, Optional

from analyzer_helper.telegram.utils.date_time_format_converter import (
    DateTimeFormatConverter,
)
from github.neo4j_storage.neo4j_connection import Neo4jConnection
from tc_hivemind_backend.db.mongo import MongoSingleton


class ExtractRawInfo:
    def __init__(self, chat_id: int, platform_id: str):
        """
        Initialize the ExtractRawInfo with the forum endpoint, platform id and set up Neo4j and MongoDB connection.
        """
        self.neo4jConnection = Neo4jConnection()
        self.driver = self.neo4jConnection.connect_neo4j()
        self.chat_id = chat_id
        self.converter = DateTimeFormatConverter()
        self.client = MongoSingleton.get_instance().client
        self.platform_db = self.client[platform_id]
        self.rawmemberactivities_collection = self.platform_db["rawmemberactivities"]

    def close(self):
        """
        Close the Neo4j connection.
        """
        self.driver.close()

    def fetch_message_details(
        self, created_at: Optional[str] = None, comparison: Optional[str] = None
    ) -> list:
        """
        Fetch details of messages from the Telegram chat.

        :param created_at: Optional datetime string to filter messages created after this date.
        :param comparison: Optional comparison operator, either 'gt' for greater than or 'gte' for greater than or equal to.
        :return: List of dictionaries containing message details.
        """
        if comparison and comparison not in {"gt", "gte"}:
            raise ValueError("comparison must be either 'gt' or 'gte'")

        where_clause = """
            WHERE c.id = $chat_id AND (message.text IS NOT NULL)
        """
        if created_at and comparison:
            operator = ">" if comparison == "gt" else ">="
            where_clause += f" AND message.date {operator} $created_at"

        query = f"""
        MATCH (c:TGChat)<-[:SENT_IN]-(message:TGMessage)
        {where_clause}
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
        OPTIONAL MATCH (author:TGUser)-[created_rel:CREATED_MESSAGE]->(message)
        OPTIONAL MATCH (reacted_user:TGUser)-[react_rel:REACTED_TO]->(message)
        OPTIONAL MATCH (reply_msg:TGMessage)-[:REPLIED]->(message)
        OPTIONAL MATCH (replied_user:TGUser)-[:CREATED_MESSAGE]->(reply_msg)
        OPTIONAL MATCH (message)-[:MENTIONED]->(mentioned_user:TGUser)
        RETURN
            message.id AS message_id,
            message_text,
            author.id AS author_id,
            message.date AS message_created_at,
            edited_at AS message_edited_at,
            [
                mention IN COLLECT({{mentioned_user_id: mentioned_user.id}}) WHERE mention.mentioned_user_id IS NOT NULL
            ] AS mentions,
            [
                reply IN COLLECT({{reply_message_id: reply_msg.id, replier_id: replied_user.id, replied_date: reply_msg.date}}) WHERE reply.reply_message_id IS NOT NULL
            ] AS replies,
            [
                reaction IN COLLECT({{reactor_id: reacted_user.id, reaction: react_rel.new_reaction, reaction_date: react_rel.date}}) WHERE reaction.reactor_id IS NOT NULL
            ] AS reactions
        ORDER BY message_created_at DESC
        """

        parameters = {"chat_id": self.chat_id}
        if created_at and comparison:
            parameters["created_at"] = created_at

        with self.driver.session() as session:
            result = session.run(query, parameters)
            messages = result.data()

        return messages

    def fetch_raw_data(
        self, created_at: Optional[str] = None, comparison: Optional[str] = None
    ) -> list:
        """
        Fetch telegram raw data.

        :param created_at: Optional datetime string to filter posts created after this date.
        :param comparison: Optional comparison operator, either 'gt' for greater than or 'gte' for greater than or equal to.
        :return: List of combined dictionaries containing telegram raw data.
        """
        if created_at and comparison:
            data = self.fetch_message_details(created_at, comparison)
        else:
            data = self.fetch_message_details()

        return data

    def fetch_message_reactions(self, message_id: int) -> List[Dict[str, Any]]:
        """
        fetch message reactions

        Parameters
        -------------
        message_id : str
            the id of message to fetch its reactions

        Returns
        ---------
        reactions : list[dict[str, any]]
            the list of reactions pointed to a message
        """
        query = """
        MATCH (u:TGUser) -[r:REACTED_TO]-> (message:TGMessage {id: $message_id})
        WITH u, MAX(r.updated_at) as reaction_time
        MATCH (u)-[r:REACTED_TO {updated_at: reaction_time}]->(message:TGMessage {id: $message_id})
        RETURN
            u.id AS reactor_id, r.updated_at AS reaction_time
        """
        with self.driver.session() as session:
            result = session.run(query, parameters={"message_id": message_id})
            reactions = result.data()

        return reactions

    def extract(self, period: datetime, recompute: bool = False) -> list:
        """
        Extract data based on the period and recompute flag.

        :param period: The datetime period to filter posts.
        :param recompute: Flag to indicate if recompute is needed.
        :return: List of combined dictionaries containing telegram raw data.
        """
        data = []
        if recompute:
            data = self.fetch_raw_data()
        else:
            latest_activity = self.rawmemberactivities_collection.find_one(
                sort=[("date", -1)]
            )
            latest_activity_date: datetime = (
                latest_activity["date"] if latest_activity else None
            )
            if latest_activity_date:
                # Convert to unix timestmap format
                latest_activity_date_timestamp = self.converter.datetime_to_timestamp(
                    latest_activity_date
                )
                period_timestamp = self.converter.datetime_to_timestamp(period)
                if latest_activity_date_timestamp >= period_timestamp:
                    data = self.fetch_raw_data(latest_activity_date_timestamp, "gt")
                else:
                    data = self.fetch_raw_data(period_timestamp, "gte")
            else:
                data = self.fetch_raw_data()
        return data
