from .neo4j_connection import Neo4jConnection
from .neo4j_enums import Node
from .utils import remove_nested_collections

def save_pull_requests_to_neo4j(pr: dict, repository_id: str):
    
    neo4jConnection = Neo4jConnection()
    driver = neo4jConnection.connect_neo4j()

    cleaned_pr = remove_nested_collections(pr)
    
    with driver.session() as session:
        session.execute_write(lambda tx: 
            tx.run(f"""
                MERGE (pr:{Node.PullRequest.value} {{id: $pr.id}})
                SET pr += $pr, pr.repository_id = $repository_id, pr.latestSavedAt = datetime()
            """, pr= cleaned_pr, repository_id= repository_id)
        )
    driver.close()