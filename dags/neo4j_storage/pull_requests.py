from .neo4j_connection import Neo4jConnection
from .neo4j_enums import Node, Relationship
from .utils import remove_nested_collections

def save_pull_request_to_neo4j(pr: dict, repository_id: str):
    
    neo4jConnection = Neo4jConnection()
    driver = neo4jConnection.connect_neo4j()

    repo_creator = pr.pop('user', None)
    assignee = pr.pop('assignee', None)
    assignees = pr.pop('assignees', None)
    requested_reviewers = pr.pop('requested_reviewers', None)
    # TODO: Do it after extracting labels
    labels = pr.pop('labels', None)
    cleaned_pr = remove_nested_collections(pr)
    

    if assignee:
        assignee_query = f"""
            WITH pr
            MERGE (ghu:{Node.GitHubUser.value} {{id: $assignee.id}})
                SET ghu += $assignee, ghu.latestSavedAt = datetime()
            WITH pr, ghu
            MERGE (pr)-[assignghu:{Relationship.ASSIGNED.value}]->(ghu)
                SET assignghu.latestSavedAt = datetime()
        """ 
    else: assignee_query = ""

    assignees_query = f"""
        WITH pr
        UNWIND $assignees as one_assignee
        MERGE (ghuoa:{Node.GitHubUser.value} {{id: one_assignee.id}})
            SET ghuoa += one_assignee, ghuoa.latestSavedAt = datetime()
        WITH pr, ghuoa
        MERGE (pr)-[assignghuoa:{Relationship.ASSIGNED.value}]->(ghuoa)
            SET assignghuoa.latestSavedAt = datetime()
    """

    requested_reviewers_query = f"""
        WITH pr
        UNWIND $requested_reviewers as requested_reviewer
        MERGE (ghurr:{Node.GitHubUser.value} {{id: requested_reviewer.id}})
            SET ghurr += requested_reviewer, ghurr.latestSavedAt = datetime()
        WITH pr, ghurr
        MERGE (pr)-[isreviewerghu:{Relationship.IS_REVIEWER.value}]->(ghurr)
            SET isreviewerghu.latestSavedAt = datetime()
    """

    with driver.session() as session:
        session.execute_write(lambda tx: 
            tx.run(f"""
                MERGE (pr:{Node.PullRequest.value} {{id: $pr.id}})
                SET pr += $pr, pr.repository_id = $repository_id, pr.latestSavedAt = datetime()
                
                WITH pr
                MERGE (ghu:{Node.GitHubUser.value} {{id: $repo_creator.id}})
                    SET ghu += $repo_creator, ghu.latestSavedAt = datetime()
                WITH pr, ghu
                MERGE (ghu)-[pc:{Relationship.CREATED.value}]->(pr)
                    SET pc.latestSavedAt = datetime()

                { assignee_query }
                { assignees_query }
                { requested_reviewers_query }

            """, pr= cleaned_pr, repository_id= repository_id, repo_creator= repo_creator, assignee= assignee, assignees= assignees, requested_reviewers= requested_reviewers)
        )
    driver.close()