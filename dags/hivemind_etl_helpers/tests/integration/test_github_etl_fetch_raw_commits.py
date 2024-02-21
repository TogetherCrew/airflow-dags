from datetime import datetime
from unittest import TestCase

from github.neo4j_storage.neo4j_connection import Neo4jConnection
from hivemind_etl_helpers.src.db.github.fetch_raw_data.commit import fetch_raw_commits


class TestFetchRawCommits(TestCase):
    def setUp(self) -> None:
        neo4j_connection = Neo4jConnection()
        self.neo4j_driver = neo4j_connection.connect_neo4j()
        with self.neo4j_driver.session() as session:
            session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE (n)"))

    def test_get_empty_results_no_from_date(self):
        repository_ids = [123]
        commits = fetch_raw_commits(repository_id=repository_ids, from_date=None)
        self.assertEqual(commits, [])

    def test_get_empty_results(self):
        repository_ids = [123]
        commits = fetch_raw_commits(
            repository_id=repository_ids, from_date=datetime(2024, 1, 1)
        )
        self.assertEqual(commits, [])

    def test_get_single_commit_single_repo_no_from_date(self):
        with self.neo4j_driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE (co:Commit)<-[:COMMITED]-(user:GitHubUser {login: "author #1"})
                        SET
                            co.`commit.author.name` = "Author#1",
                            co.`commit.message` = "Issue #1 is resolved!",
                            co.`commit.url` = "https://api.sample_url_for_commit.html",
                            co.`parents.0.html_url` = "https://sample_url_for_commit.html",
                            co.repository_id = 123,
                            co.sha = "sha#1111",
                            co.latestSavedAt = "2024-02-06T10:23:50Z",
                            co.`commit.author.date` = "2024-01-01T10:23:50Z",
                            co.`commit.verification.reason` = "valid"

                    CREATE (repo:Repository {id: 123, full_name: "Org/SampleRepo"})
                    """
                )
            )

        repository_ids = [123]
        commits = fetch_raw_commits(
            repository_id=repository_ids,
        )

        self.assertEqual(len(commits), 1)
        self.assertEqual(commits[0]["author_name"], "author #1")
        self.assertEqual(commits[0]["message"], "Issue #1 is resolved!")
        self.assertEqual(
            commits[0]["api_url"], "https://api.sample_url_for_commit.html"
        )
        self.assertEqual(commits[0]["html_url"], "https://sample_url_for_commit.html")
        self.assertEqual(commits[0]["repository_id"], 123)
        self.assertEqual(commits[0]["repository_name"], "Org/SampleRepo")
        self.assertEqual(commits[0]["sha"], "sha#1111")
        self.assertEqual(commits[0]["latest_saved_at"], "2024-02-06T10:23:50Z")
        self.assertEqual(commits[0]["created_at"], "2024-01-01T10:23:50Z")
        self.assertEqual(commits[0]["verification"], "valid")

    def test_get_single_commit_single_repo_with_from_date(self):
        with self.neo4j_driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE (co:Commit)<-[:COMMITED]-(user:GitHubUser {login: "author #1"})
                        SET
                            co.`commit.author.name` = "Author#1",
                            co.`commit.message` = "Issue #1 is resolved!",
                            co.`commit.url` = "https://api.sample_url_for_commit.html",
                            co.`parents.0.html_url` = "https://sample_url_for_commit.html",
                            co.repository_id = 123,
                            co.sha = "sha#1111",
                            co.latestSavedAt = "2024-02-06T10:23:50Z",
                            co.`commit.author.date` = "2024-01-01T10:23:50Z",
                            co.`commit.verification.reason` = "invalid"

                    CREATE (repo:Repository {id: 123, full_name: "Org/SampleRepo2"})
                    """
                )
            )

        repository_ids = [123]
        commits = fetch_raw_commits(
            repository_id=repository_ids,
            from_date=datetime(2024, 1, 1),
        )

        self.assertEqual(len(commits), 1)
        self.assertEqual(commits[0]["author_name"], "author #1")
        self.assertEqual(commits[0]["message"], "Issue #1 is resolved!")
        self.assertEqual(
            commits[0]["api_url"], "https://api.sample_url_for_commit.html"
        )
        self.assertEqual(commits[0]["html_url"], "https://sample_url_for_commit.html")
        self.assertEqual(commits[0]["repository_id"], 123)
        self.assertEqual(commits[0]["repository_name"], "Org/SampleRepo2")
        self.assertEqual(commits[0]["sha"], "sha#1111")
        self.assertEqual(commits[0]["latest_saved_at"], "2024-02-06T10:23:50Z")
        self.assertEqual(commits[0]["created_at"], "2024-01-01T10:23:50Z")
        self.assertEqual(commits[0]["verification"], "invalid")
