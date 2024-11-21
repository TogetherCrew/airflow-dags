import datetime

from analyzer_helper.discourse.utils.convert_date_time_formats import (
    DateTimeFormatConverter,
)
from github.neo4j_storage.neo4j_connection import Neo4jConnection
from tc_hivemind_backend.db.mongo import MongoSingleton


class ExtractRawMembers:
    def __init__(self, forum_endpoint: str, platform_id: str):
        """
        Initialize the ExtractRawMembers with the Neo4j connection parameters.
        """
        self.neo4jConnection = Neo4jConnection()
        self.driver = self.neo4jConnection.connect_neo4j()
        self.converter = DateTimeFormatConverter()
        self.forum_endpoint = forum_endpoint
        self.client = MongoSingleton.get_instance().client
        self.platform_db = self.client[platform_id]
        self.rawmembers_collection = self.platform_db["rawmembers"]

    def close(self):
        """
        Close the Neo4j connection.
        """
        self.driver.close()

    def fetch_member_details(self, start_date: datetime = None):
        """
        Fetch details of members from the Discourse forum.

        :param start_date: Optional datetime object to filter members created after this date.
        :return: List of dictionaries containing member details.
        """
        query = """
        MATCH (user:DiscourseUser {endpoint: $forum_endpoint})
        WHERE user.username IS NOT NULL
        """

        parameters = {"forum_endpoint": self.forum_endpoint}

        if start_date:
            query += " AND user.createdAt >= $start_date"
            parameters["start_date"] = start_date

        query += """
        RETURN
            user.id AS id,
            user.createdAt AS joined_at,
            user.name as name,
            user.avatarTemplate as avatar,
            user.username as username
        ORDER BY joined_at ASC
        """
        with self.driver.session() as session:
            result = session.run(query, parameters)
            return [record.data() for record in result]

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
                sort=[("joined_at", -1)]
            )
            latest_joined_at = (
                latest_rawmember["joined_at"] if latest_rawmember else None
            )

            if latest_joined_at:
                # Conversion to ISO format because of neo4j
                latest_joined_at_iso_format = self.converter.to_iso_format(
                    latest_joined_at
                )
                members = self.fetch_member_details(
                    start_date=latest_joined_at_iso_format
                )
            else:
                members = self.fetch_member_details()

        return members
