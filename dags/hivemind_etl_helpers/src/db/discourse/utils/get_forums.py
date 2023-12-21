import neo4j
from hivemind_etl_helpers.src.utils.neo4j import Neo4jConnection


def get_forums(community_id: str) -> list[neo4j._data.Record]:
    """
    get a list of forums from a community

    Parameters
    -----------
    community_id : str
        the community we want to process its forum

    Returns
    --------
    forums : list[neo4j._data.Record]
        a list of neo4j records, each having the uuid and endpoint of a forum
    """
    neo4j = Neo4jConnection()

    query = """
        MATCH (f:DiscourseForum) -[:IS_WITHIN]->(c:Community {id: $communityId})
        RETURN f.uuid as uuid, f.endpoint as endpoint
    """
    forums, _, _ = neo4j.neo4j_ops.neo4j_driver.execute_query(
        query, communityId=community_id
    )

    return forums
