from datetime import datetime
from unittest import TestCase

from github.neo4j_storage.neo4j_connection import Neo4jConnection
from hivemind_etl_helpers.src.db.github.extract import GithubExtraction


class TestGithubETLFetchCommentsFiltered(TestCase):
    def setUp(self) -> None:
        neo4j_connection = Neo4jConnection()
        self.neo4j_driver = neo4j_connection.connect_neo4j()
        with self.neo4j_driver.session() as session:
            session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE (n)"))
        self.extractor = GithubExtraction()

    def test_get_empty_results_no_from_date(self):
        repository_ids = [123, 124]
        comments = self.extractor.fetch_comments(
            repository_id=repository_ids,
            from_date=None,
            pr_ids=[111],
            issue_ids=[999],
        )
        self.assertEqual(comments, [])

    def test_get_empty_results(self):
        repository_ids = [123, 124]
        comments = self.extractor.fetch_comments(
            repository_id=repository_ids,
            from_date=datetime(2024, 1, 1),
            pr_ids=[111],
            issue_ids=[999],
        )
        self.assertEqual(comments, [])

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
        comments = self.extractor.fetch_comments(
            repository_id=repository_ids, from_date=datetime(2024, 1, 1)
        )

        self.assertEqual(len(comments), 1)
        self.assertEqual(comments[0].id, 111)
        self.assertEqual(comments[0].created_at, "2024-02-06 10:23:50")
        self.assertEqual(comments[0].updated_at, "2024-02-06 10:23:51")
        self.assertEqual(comments[0].repository_name, "Org/SampleRepo")
        self.assertEqual(comments[0].related_title, "sample pr title")
        self.assertEqual(comments[0].related_node, "PullRequest")
        self.assertEqual(comments[0].latest_saved_at, "2024-02-10 10:23:50")
        self.assertEqual(comments[0].text, "A sample comment")
        self.assertEqual(comments[0].url, "https://www.someurl.com")

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
            comments[0].reactions,
            expected_reactions,
        )

    def test_get_single_comment_single_repo_no_from_date_filteed_pr(self):
        with self.neo4j_driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE (c:Comment)<-[:CREATED]-(user:GitHubUser {login: "author #1"})
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

                    CREATE (pr:PullRequest {id: 111})<-[:IS_ON]-(c)
                        SET pr.title = "sample pr title"
                    CREATE (repo:Repository {id: 123, full_name: "Org/SampleRepo"})
                    """
                )
            )

        repository_ids = [123]
        comments = self.extractor.fetch_comments(
            repository_id=repository_ids,
            pr_ids=[111],
        )

        self.assertEqual(len(comments), 1)
        self.assertEqual(comments[0].id, 111)
        self.assertEqual(comments[0].author_name, "author #1")
        self.assertEqual(comments[0].repository_name, "Org/SampleRepo")
        self.assertEqual(comments[0].url, "https://www.someurl.com")
        self.assertEqual(comments[0].created_at, "2024-02-06 10:23:50")
        self.assertEqual(comments[0].updated_at, "2024-02-06 10:23:51")
        self.assertEqual(comments[0].related_title, "sample pr title")
        self.assertEqual(comments[0].related_node, "PullRequest")
        self.assertEqual(comments[0].text, "A sample comment")
        self.assertEqual(comments[0].latest_saved_at, "2024-02-10 10:23:50")
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

    def test_get_single_comment_single_repo_with_from_date_filtered_issue(self):
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

                    CREATE (c2:Comment)<-[:CREATED]-(:GitHubUser {login: "author #2"})
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

                    CREATE (i:Issue {id: 111})<-[:IS_ON]-(c)
                        SET i.title = "sample issue title"
                    CREATE (pr:PullRequest)<-[:IS_ON]-(c2)
                        SET pr.title = "sample pr title 2"
                    CREATE (repo:Repository {id: 123, full_name: "Org/SampleRepo"})
                    """
                )
            )

        repository_ids = [123]
        comments = self.extractor.fetch_comments(
            repository_id=repository_ids,
            from_date=datetime(2024, 1, 1),
            issue_ids=[111],
        )

        self.assertEqual(len(comments), 1)
        self.assertEqual(comments[0].id, 111)
        self.assertEqual(comments[0].author_name, "author #1")
        self.assertEqual(comments[0].repository_name, "Org/SampleRepo")
        self.assertEqual(comments[0].url, "https://www.someurl.com")
        self.assertEqual(comments[0].created_at, "2024-02-06 10:23:50")
        self.assertEqual(comments[0].updated_at, "2024-02-06 10:23:51")
        self.assertEqual(comments[0].related_title, "sample issue title")
        self.assertEqual(comments[0].related_node, "Issue")
        self.assertEqual(comments[0].text, "A sample comment")
        self.assertEqual(comments[0].latest_saved_at, "2024-02-10 10:23:50")
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

    def test_get_single_comment_single_repo_with_from_date_filtered_pr_issue(self):
        """
        We're choosing the pull requests and issues both
        """
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

                    CREATE (c2:Comment)<-[:CREATED]-(:GitHubUser {login: "author #2"})
                    SET
                        c2.id = 112,
                        c2.created_at = "2024-02-07T10:23:50Z",
                        c2.updated_at = "2024-02-07T10:23:51Z",
                        c2.repository_id = 123,
                        c2.body = "A sample comment",
                        c2.latestSavedAt = "2024-02-10T10:23:50Z",
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

                    CREATE (i:Issue {id: 111})<-[:IS_ON]-(c)
                        SET i.title = "sample issue title"
                    CREATE (pr:PullRequest {id: 999})<-[:IS_ON]-(c2)
                        SET pr.title = "sample pr title 2"
                    CREATE (repo:Repository {id: 123, full_name: "Org/SampleRepo"})
                    """
                )
            )

        repository_ids = [123]
        comments = self.extractor.fetch_comments(
            repository_id=repository_ids,
            from_date=datetime(2024, 1, 1),
            issue_ids=[111],
            pr_ids=[999],
        )

        self.assertEqual(len(comments), 2)
        self.assertEqual(comments[0].id, 111)
        self.assertEqual(comments[0].author_name, "author #1")
        self.assertEqual(comments[0].repository_name, "Org/SampleRepo")
        self.assertEqual(comments[0].url, "https://www.someurl.com")
        self.assertEqual(comments[0].created_at, "2024-02-06 10:23:50")
        self.assertEqual(comments[0].updated_at, "2024-02-06 10:23:51")
        self.assertEqual(comments[0].related_title, "sample issue title")
        self.assertEqual(comments[0].related_node, "Issue")
        self.assertEqual(comments[0].text, "A sample comment")
        self.assertEqual(comments[0].latest_saved_at, "2024-02-10 10:23:50")
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

        self.assertEqual(comments[1].id, 112)
        self.assertEqual(comments[1].author_name, "author #2")
        self.assertEqual(comments[1].repository_name, "Org/SampleRepo")
        self.assertEqual(comments[1].url, "https://www.someurl.com")
        self.assertEqual(comments[1].created_at, "2024-02-07 10:23:50")
        self.assertEqual(comments[1].updated_at, "2024-02-07 10:23:51")
        self.assertEqual(comments[1].related_title, "sample pr title 2")
        self.assertEqual(comments[1].related_node, "PullRequest")
        self.assertEqual(comments[1].text, "A sample comment")
        self.assertEqual(comments[1].latest_saved_at, "2024-02-10 10:23:50")
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
        self.assertEqual(comments[1].reactions, expected_reactions)
