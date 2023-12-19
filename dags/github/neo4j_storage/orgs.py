from neo4j.time import DateTime as Neo4jDateTime

from .neo4j_connection import Neo4jConnection
from .neo4j_enums import Node, Relationship


def get_orgs_profile_from_neo4j():
    neo4jConnection = Neo4jConnection()
    driver = neo4jConnection.connect_neo4j()

    def do_cypher_tx(tx, cypher):
        # TODO: should be refactored
        result = tx.run(cypher)
        values = [record for record in result]

        records = []
        for value in values:
            record = {}
            for k, v in value[0].items():
                if isinstance(v, Neo4jDateTime):
                    record[k] = v.isoformat()
                else:
                    record[k] = v
            records.append(record)

        return records

    with driver.session() as session:
        orgs = session.execute_read(
            do_cypher_tx,
            f"""
                MATCH (op:{Node.OrganizationProfile.value})
                RETURN op
            """,
        )
    driver.close()

    return orgs


def save_orgs_to_neo4j(org: dict):
    neo4jConnection = Neo4jConnection()
    driver = neo4jConnection.connect_neo4j()

    with driver.session() as session:
        session.execute_write(
            lambda tx: tx.run(
                f"""
                MERGE (gho:{Node.GitHubOrganization.value} {{id: $org.id}})
                  SET gho += $org, gho.latestSavedAt = datetime()
            """,
                org=org,
            )
        )
    driver.close()


def save_org_member_to_neo4j(org_id: str, member: dict):
    neo4jConnection = Neo4jConnection()
    driver = neo4jConnection.connect_neo4j()

    with driver.session() as session:
        session.execute_write(
            lambda tx: tx.run(
                f"""
                MATCH (gho:{Node.GitHubOrganization.value} {{id: $org_id}})
                WITH gho
                MERGE (ghu:{Node.GitHubUser.value} {{id: $member.id}})
                  SET ghu += $member, ghu.latestSavedAt = datetime()
                WITH ghu, gho
                MERGE (ghu)-[im:{Relationship.IS_MEMBER.value}]->(gho)
                  SET im.latestSavedAt = datetime()
            """,
                org_id=org_id,
                member=member,
            )
        )
    driver.close()
