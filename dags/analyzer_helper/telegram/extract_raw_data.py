from datetime import datetime
from typing import Dict, List, Optional

from analyzer_helper.telegram.utils.date_time_format_converter import (
    DateTimeFormatConverter,
)
from github.neo4j_storage.neo4j_connection import Neo4jConnection
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class ExtractRawInfo:
    def __init__(self, forum_endpoint: str, platform_id: str):
        """
        Initialize the ExtractRawInfo with the forum endpoint, platform id and set up Neo4j and MongoDB connection.
        """
        self.neo4jConnection = Neo4jConnection()
        self.driver = self.neo4jConnection.connect_neo4j()
        self.forum_endpoint = forum_endpoint
        self.converter = DateTimeFormatConverter()
        self.client = MongoSingleton.get_instance().client
        self.platform_db = self.client[platform_id]
        self.rawmemberactivities_collection = self.platform_db["rawmemberactivities"]

    def close(self):
        """
        Close the Neo4j connection.
        """
        self.driver.close()

    def fetch_message_details(self, created_at: Optional[str] = None, comparison: Optional[str] = None) -> list:
        """
        Fetch details of messages from the Telegram chat.

        :param created_at: Optional datetime string to filter messages created after this date.
        :param comparison: Optional comparison operator, either 'gt' for greater than or 'gte' for greater than or equal to.
        :return: List of dictionaries containing message details.
        """
        if comparison:
            assert comparison in {"gt", "gte"}, "comparison must be either 'gt' or 'gte'"

        where_clause = "WHERE c.title = $chat_title"
        if created_at and comparison:
            print(f"Fetching raw data with created_at: {created_at} and comparison: {comparison}")
            operator = ">" if comparison == "gt" else ">="
            where_clause += f" AND m.date {operator} $created_at"

        query = f"""
        MATCH (c:TGChat)<-[:SENT_IN]-(m:TGMessage)
        {where_clause}
        WITH m.id AS message_id, m.text AS message_text, m.date AS message_created_at, m

        // Fetch who created the message
        OPTIONAL MATCH (u:TGUser)-[r:CREATED_MESSAGE]->(m)
        WITH message_id, message_text, message_created_at, u.id AS user_id, r.date AS created_date, m

        // Fetch reactions to the message
        OPTIONAL MATCH (reactor:TGUser)-[r:REACTED_TO]->(m)
        WITH message_id, message_text, message_created_at, user_id, created_date, m,
            [reaction IN collect({{reactor_id: reactor.id, reaction: r.new_reaction, reaction_date: r.date}}) WHERE reaction.reactor_id IS NOT NULL] AS reactions

        // Fetch message replies
        OPTIONAL MATCH (m1:TGMessage)-[r:REPLIED]->(m)
        OPTIONAL MATCH (replier:TGUser)-[:CREATED_MESSAGE]->(m1)
        WITH message_id, message_text, message_created_at, user_id, created_date, reactions, m,
            [reply IN collect({{reply_message_id: m1.id, replier_id: replier.id, replied_date: m1.date}}) WHERE reply.reply_message_id IS NOT NULL] AS replies

        // Fetch mentions in the message
        OPTIONAL MATCH (m)-[r:MENTIONED]->(mentioned:TGUser)
        WITH message_id, message_text, message_created_at, user_id, created_date, reactions, replies,
            [mention IN collect({{mentioned_user_id: mentioned.id}}) WHERE mention.mentioned_user_id IS NOT NULL] AS mentions

        RETURN message_id, message_text, message_created_at, user_id, reactions, replies, mentions
        ORDER BY message_id
        LIMIT 10
        """

        parameters = {"chat_title": self.forum_endpoint}
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
            latest_activity_date = latest_activity["date"] if latest_activity else None
            if latest_activity_date is not None:
                # Convert latest_activity_date to datetime if it is a string
                if isinstance(latest_activity_date, str):
                    latest_activity_date_datetime = self.converter.string_to_datetime(latest_activity_date)
                elif isinstance(latest_activity_date, float):  # if it is a timestamp
                    latest_activity_date_datetime = self.converter.timestamp_to_datetime(latest_activity_date)
                else:
                    latest_activity_date_datetime = latest_activity_date
                # Convert latest_activity_date_datetime to unix timestmap format
                latest_activity_date_unix_timestamp_format = (
                    float(self.converter.datetime_to_timestamp(latest_activity_date_datetime))
                    if latest_activity_date_datetime
                    else None
                )
                period_unix_timestamp_format = float(self.converter.datetime_to_timestamp(period))
                print("latest_activity_date_unix_timestamp_format:", latest_activity_date_unix_timestamp_format)
                print("period_unix_timestamp_format:", period_unix_timestamp_format)
                if latest_activity_date_unix_timestamp_format >= period_unix_timestamp_format:
                    data = self.fetch_raw_data(latest_activity_date_unix_timestamp_format, "gt")
                    print("latest_activity_date is equal or greater then period")
                else:
                    data = self.fetch_raw_data(period_unix_timestamp_format, "gte")
                    print("latest_activity_date is less then period")
            else:
                data = self.fetch_raw_data()
        return data
