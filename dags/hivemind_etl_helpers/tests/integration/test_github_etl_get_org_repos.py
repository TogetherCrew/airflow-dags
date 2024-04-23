from unittest import TestCase

from github.neo4j_storage.neo4j_connection import Neo4jConnection
from hivemind_etl_helpers.src.db.github.github_organization_repos import (
    get_github_organization_repos,
)


class TestGetGitHubOrgRepos(TestCase):
    def setUp(self) -> None:
        neo4j_connection = Neo4jConnection()
        self.neo4j_driver = neo4j_connection.connect_neo4j()
        with self.neo4j_driver.session() as session:
            session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE (n)"))

    def test_fetch_empty_repos(self):
        org_id = 123
        repo_ids = get_github_organization_repos(github_organization_id=org_id)

        self.assertEqual(repo_ids, [])

    def test_fetch_single_repo(self):
        org_id = 123
        with self.neo4j_driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE (org:GitHubOrganization {id: 123})
                    CREATE (repo: GitHubRepository {id: 100})
                    CREATE (repo)-[:IS_WITHIN]->(org)
                    """
                )
            )
        repo_ids = get_github_organization_repos(github_organization_id=org_id)
        self.assertEqual(repo_ids, [100])

    def test_fetch_multiple_repo(self):
        org_id = 123
        with self.neo4j_driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE (org:GitHubOrganization {id: 123})
                    CREATE (repo: GitHubRepository {id: 100})
                    CREATE (repo2: GitHubRepository {id: 101})
                    CREATE (repo3: GitHubRepository {id: 102})
                    CREATE (repo)-[:IS_WITHIN]->(org)
                    CREATE (repo2)-[:IS_WITHIN]->(org)
                    CREATE (repo3)-[:IS_WITHIN]->(org)
                    """
                )
            )
        repo_ids = get_github_organization_repos(github_organization_id=org_id)
        self.assertEqual(set(repo_ids), set([100, 101, 102]))
