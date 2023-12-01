from .neo4j_connection import Neo4jConnection
from .neo4j_enums import Node, Relationship
from .utils import remove_nested_collections

def save_issue_to_neo4j(issue: dict, repository_id: str):
    
    neo4jConnection = Neo4jConnection()
    driver = neo4jConnection.connect_neo4j()

    issue_creator = issue.pop('user', None)
    assignee = issue.pop('assignee', None)
    assignees = issue.pop('assignees', None)
    labels = issue.pop('labels', None)
    cleaned_issue = remove_nested_collections(issue)
    
    if assignee:
      assignee_query = f"""
            WITH is
            MERGE (ghu:{Node.GitHubUser.value} {{id: $assignee.id}})
                SET ghu += $assignee, ghu.latestSavedAt = datetime()
            WITH is, ghu
            MERGE (is)-[assignghu:{Relationship.ASSIGNED.value}]->(ghu)
                SET assignghu.latestSavedAt = datetime()
        """ 
    else: assignee_query = ""

    assignees_query = f"""
        WITH is
        UNWIND $assignees as one_assignee
        MERGE (ghuoa:{Node.GitHubUser.value} {{id: one_assignee.id}})
            SET ghuoa += one_assignee, ghuoa.latestSavedAt = datetime()
        WITH is, ghuoa
        MERGE (is)-[assignghuoa:{Relationship.ASSIGNED.value}]->(ghuoa)
            SET assignghuoa.latestSavedAt = datetime()
    """

    labels_query = f"""
        WITH is
        UNWIND $labels as label
        MERGE (lb:{Node.Label.value} {{id: label.id}})
            SET lb += label, lb.latestSavedAt = datetime()
        WITH is, lb
        MERGE (is)-[haslb:{Relationship.HAS_LABEL.value}]->(lb)
            SET haslb.latestSavedAt = datetime()
    """


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

                { assignee_query }
                { assignees_query }
                { labels_query }
            """, issue= cleaned_issue, repository_id= repository_id, issue_creator= issue_creator,
                labels= labels, assignee= assignee, assignees= assignees)
        )
    driver.close()
