from unittest import TestCase

from analyzer_helper.discourse.fetch_categories import FetchDiscourseCategories
from github.neo4j_storage.neo4j_connection import Neo4jConnection


class TestDiscourseFetchingCategories(TestCase):
    def setUp(self):
        neo4jConnection = Neo4jConnection()
        self.driver = neo4jConnection.connect_neo4j()

        self.endpoint = "endpoint.com"
        self.fetcher = FetchDiscourseCategories(self.endpoint)

    def tearDown(self):
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        self.driver.close()

    def test_no_categories(self):
        categories = self.fetcher.fetch_all()

        self.assertEqual(categories, [])

    def test_single_category_available(self):
        with self.driver.session() as session:
            session.run(
                """
                CREATE (c:DiscourseCategory 
                    {
                        endpoint: $forum_endpoint,
                        color: "0088CC",
                        name: "test category",
                        descriptionText: "category description",
                        id: 1.0
                    }
                )
                """,
                {"forum_endpoint": self.endpoint},
            )

        category_ids = self.fetcher.fetch_all()

        self.assertEqual(len(category_ids), 1)
        self.assertEqual(category_ids, [1.0])

    def test_multiple_categories_available(self):
        with self.driver.session() as session:
            session.run(
                """
                CREATE (:DiscourseCategory 
                    {
                        endpoint: $forum_endpoint,
                        color: "0088CC",
                        name: "test category",
                        descriptionText: "category description",
                        id: 1.0
                    }
                )
                CREATE (:DiscourseCategory 
                    {
                        endpoint: $forum_endpoint,
                        color: "0088CC",
                        name: "test category 2",
                        descriptionText: "category description 2",
                        id: 2.0
                    }
                )
                CREATE (:DiscourseCategory 
                    {
                        endpoint: $forum_endpoint,
                        color: "0088CC",
                        name: "test category 3",
                        descriptionText: "category description 3",
                        id: 3.0
                    }
                )
                """,
                {"forum_endpoint": self.endpoint},
            )

        category_ids = self.fetcher.fetch_all()

        self.assertEqual(len(category_ids), 3)
        self.assertEqual(set(category_ids), set([1.0, 2.0, 3.0]))
