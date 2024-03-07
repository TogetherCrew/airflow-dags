import neo4j
from hivemind_etl_helpers.src.utils.neo4j import Neo4jConnection


def get_forum_uuid(forum_endpoint: str) -> list[neo4j._data.Record]:
    """
    get the forum uuid that the community connected their discourse to

    Parameters
    -----------
    forum_endpoint : str
        the forum endpoint that is related to a community

    Returns
    --------
    forums : list[neo4j._data.Record]
        a list of neo4j records, each having the uuid and endpoint of a forum
    """
    neo4j = Neo4jConnection()

    query = """
        MATCH (f:DiscourseForum) WHERE f.endpoint = $forum_endpoint
        RETURN f.uuid as uuid
    """
    forums, _, _ = neo4j.neo4j_ops.neo4j_driver.execute_query(
        query, forum_endpoint=forum_endpoint
    )

    return forums
