from pymongo import DESCENDING

from analyzer_helper.telegram.utils.date_time_format_converter import (
    DateTimeFormatConverter,
)
from github.neo4j_storage.neo4j_connection import Neo4jConnection
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class ExtractRawMembers:
    def __init__(self, chat_id: str, platform_id: str):
        """
        Initialize the ExtractRawMembers with the Neo4j connection parameters.
        """
        self.neo4jConnection = Neo4jConnection()
        self.driver = self.neo4jConnection.connect_neo4j()
        self.converter = DateTimeFormatConverter()
        self.chat_id = chat_id
        self.client = MongoSingleton.get_instance().client
        self.platform_db = self.client[platform_id]
        self.rawmembers_collection = self.platform_db["rawmembers"]

    def close(self):
        """
        Close the Neo4j connection.
        """
        self.driver.close()

    def fetch_member_details(self, start_date: float | None = None):
        """
        Fetch details of members from the Telegram group.

        :param start_date: Optional float timestamp to filter members created after this date.
        :return: List of dictionaries containing member details.
        """
        parameters = {"chat_id": self.chat_id}
        query = """
        MATCH (u:TGUser)-[r:JOINED|LEFT]->(c:TGChat)
        WHERE c.id = $chat_id
        """

        if start_date:
            query += " AND r.date > $start_date"
            parameters["start_date"] = start_date

        query += """
        MATCH (u:TGUser)-[r:JOINED]->(c:TGChat {id: $chat_id})
        WITH u, MAX(r.date) AS joined_at
        OPTIONAL MATCH (u:TGUser)-[r:LEFT]->(c:TGChat {id: $chat_id})
        WITH u, joined_at, MAX(r.date) AS left_at
        RETURN
            u.id AS id,
            joined_at,
            left_at,
            u.is_bot as is_bot,
            u.username AS username,
            u.first_name AS first_name,
            u.last_name AS last_name
        """

        with self.driver.session() as session:
            result = session.run(query, parameters)
            raw_results = list(result)

        processed_result = [record.data() for record in raw_results]
        return processed_result

    def extract(self, recompute: bool = False) -> list:
        """
        Extract members data
        if recompute = True, then extract the whole members
        else, start extracting from the latest saved member's `joined_at` date

        Note: if the user id was duplicate, then replace.
        """
        members = []
        if recompute:
            members = self.fetch_member_details()
        else:
            # Fetch the latest joined date from rawmembers collection
            latest_rawmember = self.rawmembers_collection.find_one(
                sort=[("joined_at", DESCENDING)]
            )
            latest_joined_at = (
                latest_rawmember["joined_at"] if latest_rawmember else None
            )

            if latest_joined_at:
                # Conversion to unix timestamp format because of neo4j data structure
                latest_joined_at_timestamp = self.converter.datetime_to_timestamp(latest_joined_at)
                members = self.fetch_member_details(start_date=latest_joined_at_timestamp)
            else:
                members = self.fetch_member_details()

        return members
