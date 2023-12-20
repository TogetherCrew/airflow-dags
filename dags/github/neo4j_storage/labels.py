from .neo4j_connection import Neo4jConnection
from .neo4j_enums import Node


def save_label_to_neo4j(label: dict):
    neo4jConnection = Neo4jConnection()
    driver = neo4jConnection.connect_neo4j()

    with driver.session() as session:
        session.execute_write(
            lambda tx: tx.run(
                f"""
                MERGE (lb:{Node.Label.value} {{id: $label.id}})
                  SET lb += $label, lb.latestSavedAt = datetime()
            """,
                label=label,
            )
        )
    driver.close()
