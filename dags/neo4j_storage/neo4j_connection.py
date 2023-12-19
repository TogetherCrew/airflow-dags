import os

from dotenv import load_dotenv
from neo4j import Driver, GraphDatabase


class Neo4jConnection:
    def __init__(self) -> None:
        self.neo4j_ops = self.connect_neo4j()

    def __new__(cls):
        # making it singleton
        if not hasattr(cls, "instance"):
            cls.instance = super(Neo4jConnection, cls).__new__(cls)
        return cls.instance

    def connect_neo4j(self) -> Driver:
        load_dotenv()

        protocol = os.getenv("NEO4J_PROTOCOL", "bolt")
        host = os.getenv("NEO4J_HOST", "")
        port = os.getenv("NEO4J_PORT", "")
        user = os.getenv("NEO4J_USER", "")
        password = os.getenv("NEO4J_PASSWORD", "")

        neo4j_db = os.getenv("NEO4J_DB", "neo4j")

        # Neo4j connection details
        uri = f"{protocol}://{host}:{port}"

        # Connect to the neo4j instance
        driver = GraphDatabase.driver(uri, auth=(user, password), database=neo4j_db)
        return driver
