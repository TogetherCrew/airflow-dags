from github.neo4j_storage.neo4j_connection import Neo4jConnection


class FetchDiscourseCategories:
    """Fetch all discourse categories"""

    def __init__(self, endpoint: str) -> None:
        self.endpoint = endpoint
        neo4jConnection = Neo4jConnection()
        self.driver = neo4jConnection.connect_neo4j()

    def fetch_all(self) -> list[float]:
        """
        fetch all available categories for a specific endpoint

        Returns
        ---------
        category_ids : list[float]
            a list of category ids
        """
        query = """
        MATCH (c:DiscourseCategory {endpoint: $forum_endpoint})
        RETURN c.id as category_ids
        """

        data: list[dict[str, float]]
        with self.driver.session() as session:
            result = session.run(query, {"forum_endpoint": self.endpoint})
            data = [record.data()["category_ids"] for record in result]

        return data
