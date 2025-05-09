from datetime import datetime
from unittest import TestCase

from github.neo4j_storage.neo4j_connection import Neo4jConnection
from hivemind_etl_helpers.src.db.github.extract import GithubExtraction


class TestFetchCommits(TestCase):
    def setUp(self) -> None:
        neo4j_connection = Neo4jConnection()
        self.neo4j_driver = neo4j_connection.connect_neo4j()
        self.github_extractor = GithubExtraction()
        with self.neo4j_driver.session() as session:
            session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE (n)"))

    def test_get_empty_results_no_from_date(self):
        repository_ids = [123]
        commits = self.github_extractor.fetch_commits(
            repository_id=repository_ids, from_date=None
        )
        self.assertEqual(commits, [])

    def test_get_empty_results(self):
        repository_ids = [123]
        commits = self.github_extractor.fetch_commits(
            repository_id=repository_ids, from_date=datetime(2024, 1, 1)
        )
        self.assertEqual(commits, [])

    def test_get_single_commit_single_repo_no_from_date(self):
        with self.neo4j_driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE (co:GitHubCommit)<-[:COMMITTED_BY]-(user:GitHubUser {login: "author #1"})
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
                    CREATE (co)<-[:AUTHORED_BY]-(user)

                    CREATE (pr:GitHubPullRequest {title: 'Some PR'})
                    CREATE (co)-[:IS_ON]->(pr)
                    CREATE (repo:GitHubRepository {id: 123, full_name: "Org/SampleRepo"})
                    """
                )
            )

        repository_ids = [123]
        commits = self.github_extractor.fetch_commits(
            repository_id=repository_ids,
        )

        self.assertEqual(len(commits), 1)
        self.assertEqual(commits[0].author_name, "author #1")
        self.assertEqual(commits[0].committer_name, "author #1")
        self.assertEqual(commits[0].message, "Issue #1 is resolved!")
        self.assertEqual(commits[0].api_url, "https://api.sample_url_for_commit.html")
        self.assertEqual(commits[0].url, "https://sample_url_for_commit.html")
        self.assertEqual(commits[0].repository_id, 123)
        self.assertEqual(commits[0].repository_name, "Org/SampleRepo")
        self.assertEqual(commits[0].sha, "sha#1111")
        self.assertEqual(commits[0].latest_saved_at, 1707215030.0)
        self.assertEqual(commits[0].created_at, 1704104630.0)
        self.assertEqual(commits[0].verification, "valid")
        self.assertEqual(commits[0].related_pr_title, "Some PR")

    def test_get_single_commit_single_repo_no_from_date_no_commiter(self):
        with self.neo4j_driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE (co:GitHubCommit)<-[:AUTHORED_BY]-(user:GitHubUser {login: "author #1"})
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

                    CREATE (pr:GitHubPullRequest {title: 'Some PR'})
                    CREATE (co)-[:IS_ON]->(pr)
                    CREATE (repo:GitHubRepository {id: 123, full_name: "Org/SampleRepo"})
                    """
                )
            )

        repository_ids = [123]
        commits = self.github_extractor.fetch_commits(
            repository_id=repository_ids,
        )

        self.assertEqual(len(commits), 1)
        self.assertEqual(commits[0].author_name, "author #1")
        self.assertEqual(commits[0].committer_name, None)
        self.assertEqual(commits[0].message, "Issue #1 is resolved!")
        self.assertEqual(commits[0].api_url, "https://api.sample_url_for_commit.html")
        self.assertEqual(commits[0].url, "https://sample_url_for_commit.html")
        self.assertEqual(commits[0].repository_id, 123)
        self.assertEqual(commits[0].repository_name, "Org/SampleRepo")
        self.assertEqual(commits[0].sha, "sha#1111")
        self.assertEqual(commits[0].latest_saved_at, 1707215030.0)
        self.assertEqual(commits[0].created_at, 1704104630.0)
        self.assertEqual(commits[0].verification, "valid")
        self.assertEqual(commits[0].related_pr_title, "Some PR")

    def test_get_single_commit_single_repo_with_from_date(self):
        with self.neo4j_driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE (co:GitHubCommit)<-[:COMMITTED_BY]-(:GitHubUser {login: "author #1"})
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
                    CREATE (co)<-[:AUTHORED_BY]-(:GitHubUser {login: "author #2"})

                    CREATE (pr:GitHubPullRequest {title: 'Some PR'})
                    CREATE (co)-[:IS_ON]->(pr)
                    CREATE (repo:GitHubRepository {id: 123, full_name: "Org/SampleRepo2"})
                    """
                )
            )

        repository_ids = [123]
        commits = self.github_extractor.fetch_commits(
            repository_id=repository_ids,
            from_date=datetime(2024, 1, 1),
        )

        self.assertEqual(len(commits), 1)
        self.assertEqual(commits[0].committer_name, "author #1")
        self.assertEqual(commits[0].author_name, "author #2")
        self.assertEqual(commits[0].message, "Issue #1 is resolved!")
        self.assertEqual(commits[0].api_url, "https://api.sample_url_for_commit.html")
        self.assertEqual(commits[0].url, "https://sample_url_for_commit.html")
        self.assertEqual(commits[0].repository_id, 123)
        self.assertEqual(commits[0].repository_name, "Org/SampleRepo2")
        self.assertEqual(commits[0].sha, "sha#1111")
        self.assertEqual(commits[0].latest_saved_at, 1707215030.0)
        self.assertEqual(commits[0].created_at, 1704104630.0)
        self.assertEqual(commits[0].verification, "invalid")
        self.assertEqual(commits[0].related_pr_title, "Some PR")

    def test_get_multiple_commit_multi_repo_with_from_date_filter(self):
        with self.neo4j_driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE (co:GitHubCommit)<-[:COMMITTED_BY]-(:GitHubUser {login: "author #1"})
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

                    CREATE (co)<-[:AUTHORED_BY]-(:GitHubUser {login: "author #5"})
                    CREATE (pr:GitHubPullRequest {title: 'Some PR'})
                    CREATE (co)-[:IS_ON]->(pr)

                    CREATE (co2:GitHubCommit)<-[:COMMITTED_BY]-(user2:GitHubUser {login: "author #2"})
                        SET
                            co2.`commit.author.name` = "Author#2",
                            co2.`commit.message` = "Issue #2 is resolved!",
                            co2.`commit.url` = "https://api.sample_url_for_commit2.html",
                            co2.`parents.0.html_url` = "https://sample_url_for_commit2.html",
                            co2.repository_id = 123,
                            co2.sha = "sha#2222",
                            co2.latestSavedAt = "2023-02-06T10:23:50Z",
                            co2.`commit.author.date` = "2023-01-01T10:23:50Z",
                            co2.`commit.verification.reason` = "invalid"
                    CREATE (co2)<-[:AUTHORED_BY]-(user2)

                    CREATE (pr2:GitHubPullRequest {title: 'Some PR 2'})
                    CREATE (co2)-[:IS_ON]->(pr2)

                    CREATE (co3:GitHubCommit)<-[:COMMITTED_BY]-(user3:GitHubUser {login: "author #3"})
                        SET
                            co3.`commit.author.name` = "Author#3",
                            co3.`commit.message` = "Issue #3 is resolved!",
                            co3.`commit.url` = "https://api.sample_url_for_commit3.html",
                            co3.`parents.0.html_url` = "https://sample_url_for_commit3.html",
                            co3.repository_id = 124,
                            co3.sha = "sha#3333",
                            co3.latestSavedAt = "2024-02-06T10:23:50Z",
                            co3.`commit.author.date` = "2024-01-01T10:23:50Z",
                            co3.`commit.verification.reason` = "invalid"
                    CREATE (co3)<-[:AUTHORED_BY]-(user2)

                    CREATE (pr3:GitHubPullRequest {title: 'Some PR 3'})
                    CREATE (co3)-[:IS_ON]->(pr3)
                    CREATE (:GitHubRepository {id: 123, full_name: "Org/SampleRepo2"})
                    CREATE (:GitHubRepository {id: 124, full_name: "Org/SampleRepo3"})
                    """
                )
            )

        repository_ids = [123]
        commits = self.github_extractor.fetch_commits(
            repository_id=repository_ids,
            from_date=datetime(2024, 1, 1),
        )

        self.assertEqual(len(commits), 1)
        self.assertEqual(commits[0].author_name, "author #5")
        self.assertEqual(commits[0].committer_name, "author #1")
        self.assertEqual(commits[0].message, "Issue #1 is resolved!")
        self.assertEqual(commits[0].api_url, "https://api.sample_url_for_commit.html")
        self.assertEqual(commits[0].url, "https://sample_url_for_commit.html")
        self.assertEqual(commits[0].repository_id, 123)
        self.assertEqual(commits[0].repository_name, "Org/SampleRepo2")
        self.assertEqual(commits[0].sha, "sha#1111")
        self.assertEqual(commits[0].latest_saved_at, 1707215030.0)
        self.assertEqual(commits[0].created_at, 1704104630.0)
        self.assertEqual(commits[0].verification, "invalid")
        self.assertEqual(commits[0].related_pr_title, "Some PR")

    def test_fetch_commit_no_related_pr(self):
        """
        test the case of no related pr should have `None` as related pr field
        """
        with self.neo4j_driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE (co:GitHubCommit)<-[:AUTHORED_BY]-(user:GitHubUser {login: "author #1"})
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

                    CREATE (repo:GitHubRepository {id: 123, full_name: "Org/SampleRepo"})
                    """
                )
            )

        repository_ids = [123]
        commits = self.github_extractor.fetch_commits(
            repository_id=repository_ids,
        )

        self.assertEqual(len(commits), 1)
        self.assertEqual(commits[0].author_name, "author #1")
        self.assertEqual(commits[0].committer_name, None)
        self.assertEqual(commits[0].message, "Issue #1 is resolved!")
        self.assertEqual(commits[0].api_url, "https://api.sample_url_for_commit.html")
        self.assertEqual(commits[0].url, "https://sample_url_for_commit.html")
        self.assertEqual(commits[0].repository_id, 123)
        self.assertEqual(commits[0].repository_name, "Org/SampleRepo")
        self.assertEqual(commits[0].sha, "sha#1111")
        self.assertEqual(commits[0].latest_saved_at, 1707215030.0)
        self.assertEqual(commits[0].created_at, 1704104630.0)
        self.assertEqual(commits[0].verification, "valid")
        self.assertEqual(commits[0].related_pr_title, None)
