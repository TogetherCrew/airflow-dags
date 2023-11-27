from .neo4j_connection import Neo4jConnection
from .neo4j_enums import Node, Relationship

def save_orgs_to_neo4j(org: dict):

    neo4jConnection = Neo4jConnection()
    driver = neo4jConnection.connect_neo4j()
    
    with driver.session() as session:
        session.execute_write(lambda tx: 
            tx.run(f"""
                MERGE (gho:{Node.GitHubOrganization.value} {{id: $org.id}})
                  SET gho += $org, gho.latestSavedAt = datetime()
            """, org=org)
        )
    driver.close()

def save_org_member_to_neo4j(org_id: str, member: dict):
    neo4jConnection = Neo4jConnection()
    driver = neo4jConnection.connect_neo4j()
    
    with driver.session() as session:
        session.execute_write(lambda tx: 
            tx.run(f"""
                MATCH (gho:{Node.GitHubOrganization.value} {{id: $org_id}})
                WITH gho
                MERGE (ghu:{Node.GitHubUser.value} {{id: $member.id}})
                  SET ghu += $member, ghu.latestSavedAt = datetime()
                WITH ghu, gho
                MERGE (ghu)-[im:{Relationship.IS_MEMBER.value}]->(gho)
                  SET im.latestSavedAt = datetime()
            """, org_id= org_id, member=member)
        )
    driver.close()