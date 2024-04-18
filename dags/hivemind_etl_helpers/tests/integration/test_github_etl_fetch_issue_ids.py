from datetime import datetime
from unittest import TestCase

from github.neo4j_storage.neo4j_connection import Neo4jConnection
from hivemind_etl_helpers.src.db.github.extract import GithubExtraction


class TestGithubETLFetchIssueIDs(TestCase):
    def setUp(self) -> None:
        self.extractor = GithubExtraction()
        neo4j_connection = Neo4jConnection()
        self.neo4j_driver = neo4j_connection.connect_neo4j()
        with self.neo4j_driver.session() as session:
            session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE (n)"))

    def test_get_empty_results_no_from_date(self):
        repository_ids = [123, 124]
        issue_ids = self.extractor.fetch_issue_ids(
            repository_id=repository_ids, from_date=None
        )
        self.assertEqual(issue_ids, [])

    def test_get_empty_results(self):
        repository_ids = [123, 124]
        issue_ids = self.extractor.fetch_issue_ids(
            repository_id=repository_ids, from_date=datetime(2024, 1, 1)
        )
        self.assertEqual(issue_ids, [])

    def test_get_single_issue_id_single_repo(self):
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
        issue_ids = self.extractor.fetch_issue_ids(
            repository_id=repository_ids,
        )

        self.assertEqual(len(issue_ids), 1)
        self.assertEqual(issue_ids[0].id, 21200001)

    def test_get_multiple_issue_ids_single_repo(self):
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

                    CREATE (i2:Issue)<-[:CREATED]-(:GitHubUser {login: "author #2"})
                    SET
                        i2.state_reason = "completed",
                        i2.body = "explanation of some sample issue 2",
                        i2.latestSavedAt = "2024-02-15T06:10:02.262000000Z",
                        i2.closed_at = "2024-02-10T12:56:05Z",
                        i2.comments = 0,
                        i2.created_at = "2024-02-09T10:23:50Z",
                        i2.title = "some sample title 2",
                        i2.url = "https://api.github.com/repos/GitHub/some_repo/issues/2",
                        i2.author_association = "CONTRIBUTOR",
                        i2.labels_url = "https://api.github.com/repos/GitHub/some_repo/issues/2/labels{/name}",
                        i2.number = 1,
                        i2.updated_at = "2024-02-09T12:56:05Z",
                        i2.events_url = "https://api.github.com/repos/GitHub/some_repo/issues/2/events",
                        i2.html_url = "https://github.com/GitHub/some_repo/issues/2",
                        i2.comments_url = "https://api.github.com/repos/GitHub/some_repo/issues/2/comments",
                        i2.repository_id = 123,
                        i2.id = 21200002,
                        i2.repository_url = "https://api.github.com/repos/GitHub/some_repo",
                        i2.state = "closed",
                        i2.locked = false,
                        i2.timeline_url = "https://api.github.com/repos/GitHub/some_repo/issues/2/timeline",
                        i2.node_id = "some_id2"

                    CREATE (repo:Repository {id: 123, full_name: "Org/SampleRepo"})
                    """
                )
            )

        repository_ids = [123]
        issue_ids = self.extractor.fetch_issue_ids(
            repository_id=repository_ids,
        )

        self.assertEqual(len(issue_ids), 2)
        self.assertEqual(issue_ids[0].id, 21200001)
        self.assertEqual(issue_ids[1].id, 21200002)

    def test_get_multiple_issue_ids_single_repo_with_filtering(self):
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

                    CREATE (i2:Issue)<-[:CREATED]-(:GitHubUser {login: "author #2"})
                    SET
                        i2.state_reason = "completed",
                        i2.body = "explanation of some sample issue 2",
                        i2.latestSavedAt = "2024-02-15T06:10:02.262000000Z",
                        i2.closed_at = "2024-02-10T12:56:05Z",
                        i2.comments = 0,
                        i2.created_at = "2024-02-09T10:23:50Z",
                        i2.title = "some sample title 2",
                        i2.url = "https://api.github.com/repos/GitHub/some_repo/issues/2",
                        i2.author_association = "CONTRIBUTOR",
                        i2.labels_url = "https://api.github.com/repos/GitHub/some_repo/issues/2/labels{/name}",
                        i2.number = 1,
                        i2.updated_at = "2024-02-09T12:56:05Z",
                        i2.events_url = "https://api.github.com/repos/GitHub/some_repo/issues/2/events",
                        i2.html_url = "https://github.com/GitHub/some_repo/issues/2",
                        i2.comments_url = "https://api.github.com/repos/GitHub/some_repo/issues/2/comments",
                        i2.repository_id = 123,
                        i2.id = 21200002,
                        i2.repository_url = "https://api.github.com/repos/GitHub/some_repo",
                        i2.state = "closed",
                        i2.locked = false,
                        i2.timeline_url = "https://api.github.com/repos/GitHub/some_repo/issues/2/timeline",
                        i2.node_id = "some_id2"

                    CREATE (repo:Repository {id: 123, full_name: "Org/SampleRepo"})
                    """
                )
            )

        repository_ids = [123]
        issue_ids = self.extractor.fetch_issue_ids(
            repository_id=repository_ids, from_date=datetime(2024, 2, 8)
        )

        self.assertEqual(len(issue_ids), 1)
        self.assertEqual(issue_ids[0].id, 21200002)
