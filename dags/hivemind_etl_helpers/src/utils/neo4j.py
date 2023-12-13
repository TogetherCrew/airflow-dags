import logging
import os

from dotenv import load_dotenv
from tc_neo4j_lib import Neo4jOps


class Neo4jConnection:
    def __init__(self) -> None:
        self.neo4j_ops = self.connect_neo4j()

    def __new__(cls):
        # making it singleton
        if not hasattr(cls, "instance"):
            cls.instance = super(Neo4jConnection, cls).__new__(cls)
        return cls.instance

    def connect_neo4j(self) -> Neo4jOps:
        load_dotenv()

        protocol = os.getenv("NEO4J_PROTOCOL", "")
        host = os.getenv("NEO4J_HOST", "")
        port = os.getenv("NEO4J_PORT", "")
        user = os.getenv("NEO4J_USER", "")
        password = os.getenv("NEO4J_PASSWORD", "")

        neo4j_db = os.getenv("NEO4J_DB", "")

        neo4j_ops = Neo4jOps()

        neo4j_ops.set_neo4j_db_info(
            neo4j_db_name=neo4j_db,
            neo4j_host=host,
            neo4j_password=password,
            neo4j_port=port,
            neo4j_user=user,
            neo4j_protocol=protocol,
        )

        neo4j_ops.neo4j_database_connect()
        logging.info("Neo4j Connected Successfully!")

        return neo4j_ops
