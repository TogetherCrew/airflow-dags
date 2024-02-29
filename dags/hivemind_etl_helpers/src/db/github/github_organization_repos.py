from github.neo4j_storage.neo4j_connection import Neo4jConnection


def get_github_organization_repos(github_organization_id: str) -> list[int]:
    neo4j_connection = Neo4jConnection()
    neo4j_driver = neo4j_connection.connect_neo4j()

    with neo4j_driver.session() as session:
        query = "MATCH (go:GitHubOrganization {id: $org_id})<-[:IS_WITHIN]-(repo:Repository) RETURN COLLECT(repo.id) as repoIds"
        results = session.execute_read(
            lambda tx: list(tx.run(query, org_id=github_organization_id))
        )

    # it's always one result as we applied `COLLECT` in query
    assert len(results) == 1

    repo_ids = results[0]["repoIds"]
    return repo_ids
