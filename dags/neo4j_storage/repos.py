from .neo4j_connection import Neo4jConnection
from .neo4j_enums import Node, Relationship
from .utils import flat_map

def save_repo_to_neo4j(repo: dict):

    owner = repo.pop('owner', None)
    cleaned_repo = flat_map(repo)

    neo4jConnection = Neo4jConnection()
    driver = neo4jConnection.connect_neo4j()

    with driver.session() as session:
        session.execute_write(lambda tx: 
            tx.run(f"""
                MERGE (r:{Node.Repository.value} {{id: $repo.id}})
                  SET r += $repo, r.latestSavedAt = datetime()
                WITH r
                MATCH (go:{Node.GitHubOrganization.value} {{id: $owner.id}})
                WITH r, go
                MERGE (r)-[rel:{Relationship.IS_WITHIN.value}]->(go)
                  SET rel.latestSavedAt = datetime()
            """, repo=cleaned_repo, owner=owner)
        )
    driver.close()

def save_repo_contributors_to_neo4j(contributor: dict, repository_id: str):

    neo4jConnection = Neo4jConnection()
    driver = neo4jConnection.connect_neo4j()

    with driver.session() as session:
        session.execute_write(lambda tx: 
            tx.run(f"""
                MERGE (ghu:{Node.GitHubUser.value} {{id: $member.id}})
                  SET ghu += $member, ghu.latestSavedAt = datetime()
                WITH ghu
                MATCH (r:{Node.Repository.value} {{id: $repository_id}})
                WITH ghu, r
                MERGE (ghu)-[im:{Relationship.IS_MEMBER.value}]->(r)
                  SET im.latestSavedAt = datetime()
            """, member=contributor, repository_id=repository_id)
        )
    driver.close()