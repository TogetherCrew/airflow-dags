import datetime

from analyzer_helper.telegram.utils.date_time_format_converter import (
    DateTimeFormatConverter,
)
from github.neo4j_storage.neo4j_connection import Neo4jConnection
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


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
        Fetch details of members from the Telegram group.

        :param start_date: Optional datetime object to filter members created after this date.
        :return: List of dictionaries containing member details.
        """
        query = """
        MATCH (u:TGUser)-[r:JOINED|LEFT]->(c:TGChat)
        WHERE c.title = $chat_title
        """

        if start_date:
            query += " AND u.created_at >= $start_date"

        # query += """
        # WITH u, r, 
        #      CASE WHEN type(r) = 'JOINED' THEN r.date ELSE NULL END AS join_date, 
        #      CASE WHEN type(r) = 'LEFT' THEN r.date ELSE NULL END AS leave_date
        # WITH u, 
        #      collect(join_date) AS join_dates, 
        #      collect(leave_date) AS leave_dates
        # WITH u, 
        #      [date IN join_dates WHERE date IS NOT NULL] AS valid_join_dates, 
        #      [date IN leave_dates WHERE date IS NOT NULL] AS valid_leave_dates
        # WITH u, 
        #      valid_join_dates, 
        #      valid_leave_dates, 
        #      last(valid_join_dates) AS latest_join, 
        #      last(valid_leave_dates) AS latest_leave
        # RETURN u.id AS id, 
        #        u.is_bot AS is_bot, 
        #        CASE 
        #            WHEN latest_join > latest_leave OR latest_leave IS NULL THEN latest_join 
        #            ELSE NULL 
        #        END AS joined_at, 
        #        CASE 
        #            WHEN latest_leave > latest_join OR latest_join IS NULL THEN latest_leave 
        #            ELSE NULL 
        #        END AS left_at
        # ORDER BY id
        # """
        query += """
        WITH u, r, 
            CASE WHEN type(r) = 'JOINED' THEN r.date ELSE NULL END AS join_date, 
            CASE WHEN type(r) = 'LEFT' THEN r.date ELSE NULL END AS leave_date
        WITH u, 
            collect(join_date) AS join_dates, 
            collect(leave_date) AS leave_dates
        WITH u, 
            [date IN join_dates WHERE date IS NOT NULL] AS valid_join_dates, 
            [date IN leave_dates WHERE date IS NOT NULL] AS valid_leave_dates
        WITH u, 
            valid_join_dates, 
            valid_leave_dates, 
            CASE WHEN size(valid_join_dates) > 0 THEN last(valid_join_dates) ELSE NULL END AS latest_join, 
            CASE WHEN size(valid_leave_dates) > 0 THEN last(valid_leave_dates) ELSE NULL END AS latest_leave
        RETURN u.id AS id, 
            u.is_bot AS is_bot, 
            CASE 
                WHEN latest_join IS NOT NULL AND (latest_join > latest_leave OR latest_leave IS NULL) THEN latest_join 
                WHEN latest_join = 0.0 THEN latest_join
                ELSE NULL 
            END AS joined_at, 
            CASE 
                WHEN latest_leave IS NOT NULL AND (latest_leave > latest_join OR latest_join IS NULL) THEN latest_leave 
                WHEN latest_leave = 0.0 THEN latest_leave
                ELSE NULL 
            END AS left_at
        ORDER BY id
        """

        parameters = {"chat_title": self.forum_endpoint}

        if start_date:
            parameters["start_date"] = start_date

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
            # Conversion to unix timestamp format because of neo4j
            latest_joined_at = self.converter.datetime_to_timestamp(latest_joined_at)

            if latest_joined_at:
                members = self.fetch_member_details(
                    start_date=latest_joined_at
                )
            else:
                members = self.fetch_member_details()

        return members

extract = ExtractRawMembers("TC Ingestion Pipeline", "test_platform_id")
# extract = ExtractRawMembers("TogetherCrew - Second Group", "test_platform_id")
result = extract.fetch_member_details()
print("result: \n", result)