from datetime import datetime
from unittest import TestCase

from github.neo4j_storage.neo4j_connection import Neo4jConnection
from hivemind_etl_helpers.src.db.github.extract import GithubExtraction


class TestGithubETLFetchRawIssues(TestCase):
    def setUp(self) -> None:
        self.extractor = GithubExtraction()
        neo4j_connection = Neo4jConnection()
        self.neo4j_driver = neo4j_connection.connect_neo4j()
        with self.neo4j_driver.session() as session:
            session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE (n)"))

    def test_get_empty_results_no_from_date(self):
        repository_ids = [123, 124]
        issues = self.extractor._fetch_raw_issue_ids(
            repository_id=repository_ids, from_date=None
        )
        self.assertEqual(issues, [])

    def test_get_empty_results(self):
        repository_ids = [123, 124]
        issues = self.extractor._fetch_raw_issue_ids(
            repository_id=repository_ids, from_date=datetime(2024, 1, 1)
        )
        self.assertEqual(issues, [])

    def test_get_single_issue_single_repo_minimum_info(self):
        with self.neo4j_driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE (i:Issue)<-[:CREATED]-(:GitHubUser {login: "author #1"})
                    SET
                        i.latestSavedAt = "2024-02-15T06:10:02.262000000Z",
                        i.comments = 0,
                        i.created_at = "2024-02-06T10:23:50Z",
                        i.number = 1,
                        i.updated_at = "2024-02-06T12:56:05Z",
                        i.repository_id = 123,
                        i.id = 21200001,
                        i.node_id = "some_id"

                    CREATE (repo:Repository {id: 123, full_name: "Org/SampleRepo"})
                    """
                )
            )

        repository_ids = [123]
        issues = self.extractor._fetch_raw_issues(
            repository_id=repository_ids, from_date=datetime(2024, 1, 1)
        )

        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["id"], 21200001)

    def test_get_single_issue_single_repo_complete_info(self):
        with self.neo4j_driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE (i:Issue)<-[:CREATED]-(:GitHubUser {login: "author #1"})
                    SET
                        i.state_reason = "completed",
                        i.body = "explanation of some sample issue",
                        i.latestSavedAt = "2024-02-15T06:10:02.262000000Z",
                        i.closed_at = "2024-02-06T12:56:05Z",
                        i.comments = 0,
                        i.created_at = "2024-02-06T10:23:50Z",
                        i.title = "some sample title",
                        i.url = "https://api.github.com/repos/GitHub/some_repo/issues/1",
                        i.author_association = "CONTRIBUTOR",
                        i.labels_url = "https://api.github.com/repos/GitHub/some_repo/issues/1/labels{/name}",
                        i.number = 1,
                        i.updated_at = "2024-02-06T12:56:05Z",
                        i.events_url = "https://api.github.com/repos/GitHub/some_repo/issues/1/events",
                        i.html_url = "https://github.com/GitHub/some_repo/issues/1",
                        i.comments_url = "https://api.github.com/repos/GitHub/some_repo/issues/1/comments",
                        i.repository_id = 123,
                        i.id = 21200001,
                        i.repository_url = "https://api.github.com/repos/GitHub/some_repo",
                        i.state = "closed",
                        i.locked = false,
                        i.timeline_url = "https://api.github.com/repos/GitHub/some_repo/issues/1/timeline",
                        i.node_id = "some_id"

                    CREATE (repo:Repository {id: 123, full_name: "Org/SampleRepo"})
                    """
                )
            )

        repository_ids = [123]
        issues = self.extractor._fetch_raw_issues(
            repository_id=repository_ids, from_date=datetime(2024, 1, 1)
        )

        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["id"], 21200001)
