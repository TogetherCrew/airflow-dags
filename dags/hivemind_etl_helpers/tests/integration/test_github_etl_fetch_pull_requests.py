from datetime import datetime
from unittest import TestCase

from github.neo4j_storage.neo4j_connection import Neo4jConnection
from hivemind_etl_helpers.src.db.github.extract.pull_requests import fetch_pull_requests


class TestGithubETLFetchRawComments(TestCase):
    def setUp(self) -> None:
        neo4j_connection = Neo4jConnection()
        self.neo4j_driver = neo4j_connection.connect_neo4j()
        with self.neo4j_driver.session() as session:
            session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE (n)"))

    def test_get_empty_results_no_from_date(self):
        repository_ids = [123, 124]
        prs = fetch_pull_requests(repository_id=repository_ids, from_date=None)
        self.assertEqual(prs, [])

    def test_get_empty_results(self):
        repository_ids = [123, 124]
        prs = fetch_pull_requests(
            repository_id=repository_ids, from_date=datetime(2024, 1, 1)
        )
        self.assertEqual(prs, [])

    def test_get_single_pull_requests_single_repo_no_from_date(self):
        with self.neo4j_driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE (pr:PullRequest)<-[:CREATED]-(:GitHubUser {login: "author #1"})
                    SET
                        pr.id = 111,
                        pr.repository_id = 123,
                        pr.issue_url = "https://api.github.com/issues/1",
                        pr.created_at = "2024-02-06T10:23:50Z",
                        pr.closed_at = null,
                        pr.merged_at = null,
                        pr.state = "open",
                        pr.title = "sample title",
                        pr.html_url = "https://github.com/PullRequest/1",
                        pr.latestSavedAt = "2024-02-10T10:23:50Z"

                    CREATE (repo:Repository {id: 123, full_name: "Org/SampleRepo"})
                    """
                )
            )

        repository_ids = [123]
        prs = fetch_pull_requests(
            repository_id=repository_ids,
        )

        self.assertEqual(len(prs), 1)
        self.assertEqual(prs[0].id, 111)
        self.assertEqual(prs[0].created_at, "2024-02-06 10:23:50")
        self.assertEqual(prs[0].repository_name, "Org/SampleRepo")
        self.assertEqual(prs[0].latest_saved_at, "2024-02-10 10:23:50")
        self.assertEqual(prs[0].url, "https://github.com/PullRequest/1")
        self.assertEqual(prs[0].closed_at, None)
        self.assertEqual(prs[0].merged_at, None)
        self.assertEqual(prs[0].state, "open")
        self.assertEqual(prs[0].title, "sample title")
        self.assertEqual(prs[0].issue_url, "https://api.github.com/issues/1")

    def test_get_single_pull_requests_single_repo_with_from_date(self):
        with self.neo4j_driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE (pr:PullRequest)<-[:CREATED]-(:GitHubUser {login: "author #1"})
                    SET
                        pr.id = 111,
                        pr.repository_id = 123,
                        pr.issue_url = "https://api.github.com/issues/1",
                        pr.created_at = "2024-02-06T10:23:50Z",
                        pr.closed_at = null,
                        pr.merged_at = null,
                        pr.state = "open",
                        pr.title = "sample title",
                        pr.html_url = "https://github.com/PullRequest/1",
                        pr.latestSavedAt = "2024-02-10T10:23:50Z"

                    CREATE (repo:Repository {id: 123, full_name: "Org/SampleRepo"})
                    """
                )
            )

        repository_ids = [123]
        prs = fetch_pull_requests(
            repository_id=repository_ids,
            from_date=datetime(2024, 1, 1),
        )

        self.assertEqual(len(prs), 1)
        self.assertEqual(prs[0].id, 111)
        self.assertEqual(prs[0].created_at, "2024-02-06 10:23:50")
        self.assertEqual(prs[0].repository_name, "Org/SampleRepo")
        self.assertEqual(prs[0].latest_saved_at, "2024-02-10 10:23:50")
        self.assertEqual(prs[0].url, "https://github.com/PullRequest/1")
        self.assertEqual(prs[0].closed_at, None)
        self.assertEqual(prs[0].merged_at, None)
        self.assertEqual(prs[0].state, "open")
        self.assertEqual(prs[0].title, "sample title")
        self.assertEqual(prs[0].issue_url, "https://api.github.com/issues/1")

    def test_get_multiple_pull_requests_single_repo_with_from_date(self):
        with self.neo4j_driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE (pr:PullRequest)<-[:CREATED]-(:GitHubUser {login: "author #1"})
                    SET
                        pr.id = 111,
                        pr.repository_id = 123,
                        pr.issue_url = "https://api.github.com/issues/1",
                        pr.created_at = "2024-02-06T10:23:50Z",
                        pr.closed_at = null,
                        pr.merged_at = null,
                        pr.state = "open",
                        pr.title = "sample title",
                        pr.html_url = "https://github.com/PullRequest/1",
                        pr.latestSavedAt = "2024-02-10T10:23:50Z"

                    CREATE (pr2:PullRequest)<-[:CREATED]-(:GitHubUser {login: "author #2"})
                    SET
                        pr2.id = 112,
                        pr2.repository_id = 123,
                        pr2.issue_url = "https://api.github.com/issues/2",
                        pr2.created_at = "2024-02-08T10:23:50Z",
                        pr2.closed_at = null,
                        pr2.merged_at = null,
                        pr2.state = "open",
                        pr2.title = "sample title #2",
                        pr2.html_url = "https://github.com/PullRequest/2",
                        pr2.latestSavedAt = "2024-02-10T10:23:50Z"

                    CREATE (pr3:PullRequest)<-[:CREATED]-(:GitHubUser {login: "author #3"})
                    SET
                        pr3.id = 113,
                        pr3.repository_id = 123,
                        pr3.issue_url = "https://api.github.com/issues/3",
                        pr3.created_at = "2023-02-06T10:23:50Z",
                        pr3.closed_at = null,
                        pr3.merged_at = null,
                        pr3.state = "open",
                        pr3.title = "sample title #3",
                        pr3.html_url = "https://github.com/PullRequest/3",
                        pr3.latestSavedAt = "2024-02-10T10:23:50Z"
                    CREATE (repo:Repository {id: 123, full_name: "Org/SampleRepo"})
                    """
                )
            )

        repository_ids = [123]
        prs = fetch_pull_requests(
            repository_id=repository_ids,
            from_date=datetime(2024, 1, 1),
        )
        # note: the from_date is comparing to `closed_at` and `merged_at`
        # if they were null, then we could have more activity on pr
        # so we're getting all the 3 prs here

        self.assertEqual(len(prs), 3)
        self.assertEqual(prs[0].id, 113)
        self.assertEqual(prs[0].created_at, "2023-02-06 10:23:50")
        self.assertEqual(prs[0].repository_name, "Org/SampleRepo")
        self.assertEqual(prs[0].latest_saved_at, "2024-02-10 10:23:50")
        self.assertEqual(prs[0].url, "https://github.com/PullRequest/3")
        self.assertEqual(prs[0].closed_at, None)
        self.assertEqual(prs[0].merged_at, None)
        self.assertEqual(prs[0].state, "open")
        self.assertEqual(prs[0].title, "sample title #3")
        self.assertEqual(prs[0].issue_url, "https://api.github.com/issues/3")

        self.assertEqual(prs[1].id, 111)
        self.assertEqual(prs[1].created_at, "2024-02-06 10:23:50")
        self.assertEqual(prs[1].repository_name, "Org/SampleRepo")
        self.assertEqual(prs[1].latest_saved_at, "2024-02-10 10:23:50")
        self.assertEqual(prs[1].url, "https://github.com/PullRequest/1")
        self.assertEqual(prs[1].closed_at, None)
        self.assertEqual(prs[1].merged_at, None)
        self.assertEqual(prs[1].state, "open")
        self.assertEqual(prs[1].title, "sample title")
        self.assertEqual(prs[1].issue_url, "https://api.github.com/issues/1")

        self.assertEqual(prs[2].id, 112)
        self.assertEqual(prs[2].created_at, "2024-02-08 10:23:50")
        self.assertEqual(prs[2].repository_name, "Org/SampleRepo")
        self.assertEqual(prs[2].latest_saved_at, "2024-02-10 10:23:50")
        self.assertEqual(prs[2].url, "https://github.com/PullRequest/2")
        self.assertEqual(prs[2].closed_at, None)
        self.assertEqual(prs[2].merged_at, None)
        self.assertEqual(prs[2].state, "open")
        self.assertEqual(prs[2].title, "sample title #2")
        self.assertEqual(prs[2].issue_url, "https://api.github.com/issues/2")

