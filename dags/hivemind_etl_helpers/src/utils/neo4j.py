from tc_neo4j_lib import Neo4jOps


class Neo4jConnection:
    def __init__(self) -> None:
        self.neo4j_ops = Neo4jOps.get_instance()