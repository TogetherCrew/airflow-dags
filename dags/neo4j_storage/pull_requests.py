from .neo4j_connection import Neo4jConnection
from .neo4j_enums import Node

def save_pull_requests_to_neo4j(pr: dict, repository_id: str):
    
    neo4jConnection = Neo4jConnection()
    driver = neo4jConnection.connect_neo4j()

    del pr['user']
    del pr['head']
    del pr['base']
    del pr['_links']
    del pr["assignees"]
    del pr["requested_reviewers"]
    del pr["requested_teams"]
    del pr["labels"]
    
    with driver.session() as session:
        session.execute_write(lambda tx: 
            tx.run(f"""
                MERGE (pr:{Node.PullRequest.value} {{id: $pr.id}})
                SET pr += $pr, pr.repository_id = $repository_id, pr.latestSavedAt = datetime()
            """, pr= pr, repository_id= repository_id)
        )
    driver.close()