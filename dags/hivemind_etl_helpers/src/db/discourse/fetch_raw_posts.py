from datetime import datetime

import neo4j

from hivemind_etl_helpers.src.utils.neo4j import Neo4jConnection


def fetch_raw_posts(
    forum_id: str, from_date: datetime | None = None
) -> list[neo4j._data.Record]:
    """
    fetch raw posts from discourse neo4j database

    Parameters
    ------------
    forum_id : str
        the id of the forum we want to process its data
    from_date : datetime | None
        the posts to retrieve from a specific date
        default is `None` meaning to fetch all posts

    Returns
    ---------
    raw_records : list[neo4j._data.Record]
        list of neo4j records as the result
    """
    neo4j = Neo4jConnection()

    query = """
        MATCH (p:DiscoursePost {forumUuid: $forum_id})
        MATCH (f:DiscourseForum {uuid: $forum_id})
        WITH p, f.endpoint AS forum_endpoint
    """
    if from_date is not None:
        query += """
            WHERE 
                datetime(p.updatedAt) >= datetime($from_date)
            WITH p, forum_endpoint
        """

    # Adding the other part of query
    query += """
        MATCH (author:DiscourseUser)-[:POSTED]->(p)
        WITH author, p, forum_endpoint
        OPTIONAL MATCH (u:DiscourseUser)-[:LIKED]->(p)
        WITH author, p, forum_endpoint, COLLECT(u.username) AS liker_usernames, COLLECT(u.name) AS liker_names
        OPTIONAL MATCH (t:DiscourseTopic {id: p.topicId})
        OPTIONAL MATCH (c:DiscourseCategory)-[:HAS_TOPIC]->(t)
        OPTIONAL MATCH (pr:DiscoursePost)-[:REPLIED_TO]->(p)
        OPTIONAL MATCH (replier_user: DiscourseUser)-[:POSTED]->(pr)
        RETURN
            author.username AS author_username,
            author.name AS author_name,
            t.title AS topic,
            p.id AS postId,
            forum_endpoint,
            p.raw AS raw,
            p.createdAt AS createdAt,
            p.updatedAt AS updatedAt,
            author.trustLevel AS authorTrustLevel,
            liker_usernames,
            liker_names,
            COLLECT(c.name) AS categories,
            COLLECT(replier_user.username) AS replier_usernames,
            COLLECT(replier_user.name) AS replier_names
        ORDER BY createdAt
    """
    raw_records, _, _ = neo4j.neo4j_ops.neo4j_driver.execute_query(
        query, from_date=from_date, forum_id=forum_id
    )

    return raw_records
