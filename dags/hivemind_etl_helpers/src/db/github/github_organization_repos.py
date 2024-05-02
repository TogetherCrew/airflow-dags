import logging

from github.neo4j_storage.neo4j_connection import Neo4jConnection
from github.neo4j_storage.neo4j_enums import Node


def get_github_organization_repos(github_organization_ids: list[str]) -> list[int]:
    """
    get repositories of given organization id list

    Parameters
    ------------
    github_organization_ids : list[str]
        a list of github organization to fetch their repositories

    Returns
    ---------
    repo_ids : list[int]
        fetched repository ids from organizations
    """
    neo4j_connection = Neo4jConnection()
    neo4j_driver = neo4j_connection.connect_neo4j()

    with neo4j_driver.session() as session:
        query = (
            f"MATCH (go:{Node.GitHubOrganization.value})"
            f"<-[:IS_WITHIN]-(repo:{Node.Repository.value})"
            f"WHERE go.id IN $org_ids"
            " RETURN COLLECT(repo.id) as repoIds"
        )
        try:
            results = session.execute_read(
                lambda tx: list(tx.run(query, org_ids=github_organization_ids))
            )
        except Exception as exp:
            logging.error(
                "Failed to execute Neo4j get organization repos query!"
                "Returning empty array"
                f"| Exception: {exp}"
            )
            return []

    # it's always one result as we applied `COLLECT` in query
    assert len(results) == 1

    repo_ids = results[0]["repoIds"]
    return repo_ids
