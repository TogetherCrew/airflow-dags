from datetime import datetime
from unittest import TestCase

from github.neo4j_storage.neo4j_connection import Neo4jConnection
from hivemind_etl_helpers.src.db.github.extract import GithubExtraction


class TestGithubETLFetchComments(TestCase):
    def setUp(self) -> None:
        neo4j_connection = Neo4jConnection()
        self.neo4j_driver = neo4j_connection.connect_neo4j()
        with self.neo4j_driver.session() as session:
            session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE (n)"))
        self.extractor = GithubExtraction()

    def test_get_empty_results_no_from_date(self):
        repository_ids = [123, 124]
        comments = self.extractor.fetch_comments(
            repository_id=repository_ids, from_date=None
        )
        self.assertEqual(comments, [])

    def test_get_empty_results(self):
        repository_ids = [123, 124]
        comments = self.extractor.fetch_comments(
            repository_id=repository_ids, from_date=datetime(2024, 1, 1)
        )
        self.assertEqual(comments, [])

    def test_get_single_comment_single_repo_no_from_date(self):
        with self.neo4j_driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE (c:GitHubComment)<-[:CREATED]-(user:GitHubUser {login: "author #1"})
                    SET
                        c.id = 111,
                        c.created_at = "2024-02-06T10:23:50Z",
                        c.updated_at = "2024-02-06T10:23:51Z",
                        c.repository_id = 123,
                        c.body = "A sample comment",
                        c.latestSavedAt = "2024-02-10T10:23:50Z",
                        c.html_url = "https://www.someurl.com",
                        c.`reactions.hooray` = 0,
                        c.`reactions.eyes` = 1,
                        c.`reactions.heart` = 0,
                        c.`reactions.laugh` = 0,
                        c.`reactions.confused` = 2,
                        c.`reactions.rocket` = 1,
                        c.`reactions.+1` = 1,
                        c.`reactions.-1` = 0,
                        c.`reactions.total_count` = 5

                    CREATE (pr:GitHubPullRequest)<-[:IS_ON]-(c)
                        SET pr.title = "sample pr title"
                    CREATE (repo:GitHubRepository {id: 123, full_name: "Org/SampleRepo"})
                    """
                )
            )

        repository_ids = [123]
        comments = self.extractor.fetch_comments(repository_id=repository_ids)

        self.assertEqual(len(comments), 1)
        self.assertEqual(comments[0].id, 111)
        self.assertEqual(comments[0].author_name, "author #1")
        self.assertEqual(comments[0].repository_name, "Org/SampleRepo")
        self.assertEqual(comments[0].url, "https://www.someurl.com")
        self.assertEqual(comments[0].created_at, 1707215030.0)
        self.assertEqual(comments[0].updated_at, 1707215031.0)
        self.assertEqual(comments[0].related_title, "sample pr title")
        self.assertEqual(comments[0].related_node, "GitHubPullRequest")
        self.assertEqual(comments[0].text, "A sample comment")
        self.assertEqual(comments[0].latest_saved_at, 1707560630.0)
        expected_reactions = {
            "hooray": 0,
            "eyes": 1,
            "heart": 0,
            "laugh": 0,
            "confused": 2,
            "rocket": 1,
            "plus1": 1,
            "minus1": 0,
            "total_count": 5,
        }
        self.assertEqual(comments[0].reactions, expected_reactions)

    def test_get_single_comment_single_repo_with_from_date_data_filtered(self):
        with self.neo4j_driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE (c:GitHubComment)<-[:CREATED]-(:GitHubUser {login: "author #1"})
                    SET
                        c.id = 111,
                        c.created_at = "2024-02-06T10:23:50Z",
                        c.updated_at = "2024-02-06T10:23:51Z",
                        c.repository_id = 123,
                        c.body = "A sample comment",
                        c.latestSavedAt = "2024-02-10T10:23:50Z",
                        c.html_url = "https://www.someurl.com",
                        c.`reactions.hooray` = 0,
                        c.`reactions.eyes` = 1,
                        c.`reactions.heart` = 0,
                        c.`reactions.laugh` = 0,
                        c.`reactions.confused` = 2,
                        c.`reactions.rocket` = 1,
                        c.`reactions.+1` = 1,
                        c.`reactions.-1` = 0,
                        c.`reactions.total_count` = 5

                    CREATE (c2:GitHubComment)<-[:CREATED]-(:GitHubUser {login: "author #2"})
                    SET
                        c2.id = 112,
                        c2.created_at = "2023-02-06T10:23:50Z",
                        c2.updated_at = "2023-02-06T10:23:51Z",
                        c2.repository_id = 123,
                        c2.body = "A sample comment",
                        c2.latestSavedAt = "2023-02-10T10:23:50Z",
                        c2.html_url = "https://www.someurl.com",
                        c2.`reactions.hooray` = 0,
                        c2.`reactions.eyes` = 1,
                        c2.`reactions.heart` = 0,
                        c2.`reactions.laugh` = 0,
                        c2.`reactions.confused` = 2,
                        c2.`reactions.rocket` = 1,
                        c2.`reactions.+1` = 1,
                        c2.`reactions.-1` = 0,
                        c2.`reactions.total_count` = 5

                    CREATE (pr:GitHubPullRequest)<-[:IS_ON]-(c)
                        SET pr.title = "sample pr title"
                    CREATE (pr2:GitHubPullRequest)<-[:IS_ON]-(c2)
                        SET pr2.title = "sample pr title 2"
                    CREATE (repo:GitHubRepository {id: 123, full_name: "Org/SampleRepo"})
                    """
                )
            )

        repository_ids = [123]
        comments = self.extractor.fetch_comments(
            repository_id=repository_ids, from_date=datetime(2024, 1, 1)
        )

        self.assertEqual(len(comments), 1)
        self.assertEqual(comments[0].id, 111)
        self.assertEqual(comments[0].author_name, "author #1")
        self.assertEqual(comments[0].repository_name, "Org/SampleRepo")
        self.assertEqual(comments[0].url, "https://www.someurl.com")
        self.assertEqual(comments[0].created_at, 1707215030.0)
        self.assertEqual(comments[0].updated_at, 1707215031.0)
        self.assertEqual(comments[0].related_title, "sample pr title")
        self.assertEqual(comments[0].related_node, "GitHubPullRequest")
        self.assertEqual(comments[0].text, "A sample comment")
        self.assertEqual(comments[0].latest_saved_at, 1707560630.0)
        expected_reactions = {
            "hooray": 0,
            "eyes": 1,
            "heart": 0,
            "laugh": 0,
            "confused": 2,
            "rocket": 1,
            "plus1": 1,
            "minus1": 0,
            "total_count": 5,
        }
        self.assertEqual(comments[0].reactions, expected_reactions)
