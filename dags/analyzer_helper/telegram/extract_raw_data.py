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
            operator = ">" if comparison == "gt" else ">="
            where_clause += f" AND m.created_at {operator} $created_at"

        query = f"""
        MATCH (c:TGChat)<-[:SENT_IN]-(m:TGMessage)
        {where_clause}
        WITH m.id AS message_id, m.text AS message_text, m

        // Fetch who created the message
        OPTIONAL MATCH (u:TGUser)-[r:CREATED_MESSAGE]->(m)
        WITH message_id, message_text, u.id AS user_id, r.date AS created_date, m

        // Fetch reactions to the message
        OPTIONAL MATCH (reactor:TGUser)-[r:REACTED_TO]->(m)
        WITH message_id, message_text, user_id, created_date, m,
             collect({{reactor_id: reactor.id, reaction: r.new_reaction, reaction_date: r.date}}) AS reactions

        // Fetch message replies
        OPTIONAL MATCH (m1:TGMessage)-[r:REPLIED]->(m)
        OPTIONAL MATCH (replier:TGUser)-[:CREATED_MESSAGE]->(m1)
        WITH message_id, message_text, user_id, created_date, reactions, m,
             collect({{reply_message_id: m1.id, replier_id: replier.id, replied_date: r.date}}) AS replies

        // Fetch mentions in the message
        OPTIONAL MATCH (m)-[r:MENTIONED]->(mentioned:TGUser)
        WITH message_id, message_text, user_id, created_date, reactions, replies,
             collect({{mentioned_user_id: mentioned.id}}) AS mentions

        RETURN message_id, message_text, user_id, created_date, reactions, replies, mentions
        ORDER BY message_id
        LIMIT 10
        """

        parameters = {"chat_title": self.forum_endpoint}
        if created_at and comparison:
            parameters["created_at"] = created_at

        # Log the query and parameters for debugging
        print("Query:", query)
        print("Parameters:", parameters)

        with self.driver.session() as session:
            result = session.run(query, parameters)
            messages = result.data()

        return messages

    def fetch_post_categories(self, post_ids):
        """
        Fetch categories associated with given post IDs.

        :param post_ids: List of post IDs.
        :return: List of dictionaries containing post categories.
        """
        query = """
        MATCH (post:DiscoursePost)
        WHERE post.id in $post_ids
        MATCH (topic:DiscourseTopic)-[:HAS_POST]->(post)
        OPTIONAL MATCH (category:DiscourseCategory)-[:HAS_TOPIC]->(topic)
        RETURN
        post.id AS post_id,
        category.id AS category_id
        """
        with self.driver.session() as session:
            result = session.run(query, post_ids=post_ids)
            records = [record.data() for record in result]
            self.driver.close()
            return records

    def get_latest_post_created_at(self, forum_endpoint: str) -> Optional[str]:
        """
        Fetches the created_at timestamp of the latest post from a specified forum.

        Args:
            uri (str): The URI to connect to the Neo4j database.
            user (str): The username for the Neo4j database.
            password (str): The password for the Neo4j database.
            forum_endpoint (str): The endpoint of the Discourse forum.

        Returns:
            Optional[str]: The created_at timestamp of the latest post, or None if no posts are found.
        """
        query = """
        MATCH (forum:DiscourseForum {endpoint: $forum_endpoint})
        WITH forum
        MATCH (topic:DiscourseTopic {forumUuid: forum.uuid})
        MATCH (topic)-[:HAS_POST]->(post:DiscoursePost)
        RETURN post.createdAt AS created_at
        ORDER BY post.createdAt DESC
        LIMIT 1
        """
        with self.driver.session() as session:
            result = session.run(query, forum_endpoint=forum_endpoint)
            latest_post = result.single()
            self.driver.close()
        if latest_post:
            return latest_post["created_at"]
        else:
            return None

    def combine_posts_with_categories(
        self, post_details: List[Dict[str, any]], post_categories: List[Dict[str, any]]
    ) -> List[Dict[str, any]]:
        """
        Combine post details with their respective categories.

        :param post_details: List of dictionaries containing post details.
        :param post_categories: List of dictionaries containing post categories.
        :return: List of combined dictionaries.
        """
        category_dict = {category["post_id"]: category for category in post_categories}

        combined_results = []
        for post in post_details:
            category = category_dict.get(
                post["post_id"], {"category_id": None, "category_name": None}
            )
            post["category_id"] = category["category_id"]
            combined_results.append(post)

        return combined_results

    def fetch_raw_data(
        self, created_at: Optional[str] = None, comparison: Optional[str] = None
    ) -> list:
        """
        Fetch and combine post details and categories.

        :param created_at: Optional datetime string to filter posts created after this date.
        :param comparison: Optional comparison operator, either 'gt' for greater than or 'gte' for greater than or equal to.
        :return: List of combined dictionaries containing post details and categories.
        """
        if created_at and comparison:
            post_details = self.fetch_post_details(created_at, comparison)
        else:
            post_details = self.fetch_post_details()

        post_ids = [post["post_id"] for post in post_details]
        post_categories = self.fetch_post_categories(post_ids)
        return self.combine_posts_with_categories(post_details, post_categories)

    def extract(self, period: datetime, recompute: bool = False) -> list:
        """
        Extract data based on the period and recompute flag.

        :param period: The datetime period to filter posts.
        :param recompute: Flag to indicate if recompute is needed.
        :return: List of combined dictionaries containing post details and categories.
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
                # Convert latest_activity_date string to datetime
                latest_activity_date_datetime = (
                    self.converter.datetime_to_timestamp(latest_activity_date)
                    if latest_activity_date
                    else None
                )
                # Convert latest_activity_date_datetime to ISO format with milliseconds
                latest_activity_date_iso_format = (
                    self.converter.datetime_to_timestamp(latest_activity_date_datetime)
                    if latest_activity_date_datetime
                    else None
                )
                period_iso_format = self.converter.datetime_to_timestamp(period)
                if latest_activity_date_iso_format >= period_iso_format:
                    data = self.fetch_raw_data(latest_activity_date_iso_format, "gt")
                else:
                    data = self.fetch_raw_data(period_iso_format, "gte")
            else:
                data = self.fetch_raw_data()
        return data

if __name__ == "__main__":
    fetcher = ExtractRawInfo(forum_endpoint="TC Ingestion Pipeline", platform_id="test_platform_id")
    messages = fetcher.fetch_message_details()
    print(messages)
    fetcher.close()