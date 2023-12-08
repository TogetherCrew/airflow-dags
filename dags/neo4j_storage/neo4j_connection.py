from neo4j import GraphDatabase, Driver


class Neo4jConnection:
    def __init__(self) -> None:
        self.neo4j_ops = self.connect_neo4j()

    def __new__(cls):
        # making it singleton
        if not hasattr(cls, "instance"):
            cls.instance = super(Neo4jConnection, cls).__new__(cls)
        return cls.instance

    def connect_neo4j(self) -> Driver:
        # Neo4j connection details
        uri = "bolt://host.docker.internal:7687"
        username = "neo4j"
        password = "neo4j123456"

        # Connect to the neo4j instance
        driver = GraphDatabase.driver(uri, auth=(username, password), database="neo4j")
        return driver
