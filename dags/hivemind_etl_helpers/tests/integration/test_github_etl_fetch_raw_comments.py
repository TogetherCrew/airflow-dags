from datetime import datetime
from unittest import TestCase

from github.neo4j_storage.neo4j_connection import Neo4jConnection
from hivemind_etl_helpers.src.db.github.extract.comments import fetch_raw_comments


class TestGithubETLFetchRawComments(TestCase):
    def setUp(self) -> None:
        neo4j_connection = Neo4jConnection()
        self.neo4j_driver = neo4j_connection.connect_neo4j()
        with self.neo4j_driver.session() as session:
            session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE (n)"))

    def test_get_empty_results_no_from_date(self):
        repository_ids = [123, 124]
        comments = fetch_raw_comments(repository_id=repository_ids, from_date=None)
        self.assertEqual(comments, [])

    def test_get_empty_results(self):
        repository_ids = [123, 124]
        comments = fetch_raw_comments(
            repository_id=repository_ids, from_date=datetime(2024, 1, 1)
        )
        self.assertEqual(comments, [])

    def test_get_single_comment_single_repo_no_from_date(self):
        with self.neo4j_driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE (c:Comment)<-[:CREATED]-(:GitHubUser {login: "author #1"})
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

                    CREATE (pr:PullRequest)<-[:IS_ON]-(c)
                        SET pr.title = "sample pr title"
                    CREATE (repo:Repository {id: 123, full_name: "Org/SampleRepo"})
                    """
                )
            )

        repository_ids = [123]
        comments = fetch_raw_comments(
            repository_id=repository_ids,
        )

        self.assertEqual(len(comments), 1)
        self.assertEqual(comments[0]["id"], 111)
        self.assertEqual(comments[0]["created_at"], "2024-02-06T10:23:50Z")
        self.assertEqual(comments[0]["updated_at"], "2024-02-06T10:23:51Z")
        self.assertEqual(comments[0]["repository_name"], "Org/SampleRepo")
        self.assertEqual(comments[0]["latest_saved_at"], "2024-02-10T10:23:50Z")
        self.assertEqual(comments[0]["related_title"], "sample pr title")
        self.assertEqual(comments[0]["related_node"], "PullRequest")
        self.assertEqual(comments[0]["text"], "A sample comment")
        self.assertEqual(comments[0]["url"], "https://www.someurl.com")

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
        self.assertEqual(
            comments[0]["reactions"],
            expected_reactions,
        )

    def test_get_single_comment_single_repo_with_from_date(self):
        with self.neo4j_driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE (c:Comment)<-[:CREATED]-(:GitHubUser {login: "author #1"})
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

                    CREATE (pr:PullRequest)<-[:IS_ON]-(c)
                        SET pr.title = "sample pr title"
                    CREATE (repo:Repository {id: 123, full_name: "Org/SampleRepo"})
                    """
                )
            )

        repository_ids = [123]
        comments = fetch_raw_comments(
            repository_id=repository_ids,
            from_date=datetime(2024, 1, 1),
        )

        self.assertEqual(len(comments), 1)
        self.assertEqual(comments[0]["id"], 111)
        self.assertEqual(comments[0]["created_at"], "2024-02-06T10:23:50Z")
        self.assertEqual(comments[0]["updated_at"], "2024-02-06T10:23:51Z")
        self.assertEqual(comments[0]["repository_name"], "Org/SampleRepo")
        self.assertEqual(comments[0]["related_title"], "sample pr title")
        self.assertEqual(comments[0]["related_node"], "PullRequest")
        self.assertEqual(comments[0]["latest_saved_at"], "2024-02-10T10:23:50Z")
        self.assertEqual(comments[0]["text"], "A sample comment")
        self.assertEqual(comments[0]["url"], "https://www.someurl.com")

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
        self.assertEqual(
            comments[0]["reactions"],
            expected_reactions,
        )
