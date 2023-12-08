from .neo4j_connection import Neo4jConnection
from .neo4j_enums import Node, Relationship
from .utils import flat_map

def save_commit_to_neo4j(commit: dict, repository_id: str):
    
    neo4jConnection = Neo4jConnection()
    driver = neo4jConnection.connect_neo4j()

    committer = commit.pop('committer', None)
    cleaned_commit = flat_map(commit)
    
    if committer:
        committer_query = f"""
            WITH c
            MERGE (ghu:{Node.GitHubUser.value} {{id: $committer.id}})
                SET ghu += $committer, ghu.latestSavedAt = datetime()
            WITH c, ghu
            MERGE (ghu)-[cc:{Relationship.COMMITTED.value}]->(c)
                SET cc.latestSavedAt = datetime()
        """ 
    else: committer_query = ""


    with driver.session() as session:
        session.execute_write(lambda tx: 
            tx.run(f"""
                MERGE (c:{Node.Commit.value} {{sha: $commit.sha}})
                SET c += $commit, c.repository_id = $repository_id, c.latestSavedAt = datetime()

                { committer_query }
            """, commit= cleaned_commit, repository_id= repository_id, committer= committer))

    driver.close()

