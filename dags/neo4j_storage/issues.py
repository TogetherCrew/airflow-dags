from .neo4j_connection import Neo4jConnection
from .neo4j_enums import Node, Relationship
from .utils import remove_nested_collections

def save_issue_to_neo4j(issue: dict, repository_id: str):
    
    neo4jConnection = Neo4jConnection()
    driver = neo4jConnection.connect_neo4j()

    issue_creator = issue.pop('user', None)
    cleaned_issue = remove_nested_collections(issue)
    
    with driver.session() as session:
        session.execute_write(lambda tx: 
            tx.run(f"""
                MERGE (is:{Node.Issue.value} {{id: $issue.id}})
                SET is += $issue, is.repository_id = $repository_id, is.latestSavedAt = datetime()
                WITH is
                MERGE (ghu:{Node.GitHubUser.value} {{id: $issue_creator.id}})
                  SET ghu += $issue_creator, ghu.latestSavedAt = datetime()
                WITH is, ghu
                MERGE (ghu)-[ic:{Relationship.CREATED.value}]->(is)
                  SET ic.latestSavedAt = datetime()
            """, issue= cleaned_issue, repository_id= repository_id, issue_creator= issue_creator)
        )
    driver.close()
