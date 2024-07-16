from github.neo4j_storage.neo4j_connection import Neo4jConnection

class ExtractRawMembers:
    def __init__(self, forum_endpoint:str):
        """
        Initialize the ExtractRawMembers with the Neo4j connection parameters.
        """
        self.neo4jConnection = Neo4jConnection()
        self.driver = self.neo4jConnection.connect_neo4j()
        self.forum_endpoiont = forum_endpoint

    def close(self):
        """
        Close the Neo4j connection.
        """
        self.driver.close()

    def fetch_member_details(self):
        """
        Fetch details of members from the Discourse forum.

        :return: List of dictionaries containing member details.
        """
        query = """
        MATCH (forum:DiscourseForum {endpoint: $forum_endpoint})
        MATCH (user:DiscourseUser)-[:HAS_JOINED]->(forum)
        MATCH (user:DiscourseUser)-[:HAS_BADGE]->(badge)
        WHERE user.id IS NOT NULL
        WITH user, collect(badge.id) AS badgeIds
        RETURN id(user) AS id, user.avatarTemplate AS avatar, user.createdAt as createdAt, badgeIds
        """
        with self.driver.session() as session:
            result = session.run(query)
            return [record.data() for record in result]

# Create an instance of ExtractRawMembers
extractor = ExtractRawMembers()

try:
    # Call the fetch_member_details method
    member_details = extractor.fetch_member_details()
    print(member_details)
    # for member in member_details:
    #     print(member)
finally:
    # Ensure the Neo4j connection is closed
    extractor.close()
