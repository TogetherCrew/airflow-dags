from datetime import datetime

import neo4j
from hivemind_etl_helpers.src.utils.neo4j import Neo4jConnection


def fetch_raw_posts(
    forum_endpoint: str, from_date: datetime | None = None
) -> list[neo4j._data.Record]:
    """
    fetch raw posts from discourse neo4j database

    Parameters
    ------------
    forum_endpoint : str
        the forum endpoint we want to process its data
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
    MATCH (p:DiscoursePost {endpoint: $forum_endpoint})
    """
    if from_date:
        query += """
            WHERE
                datetime(p.createdAt) >= datetime($from_date)
                AND p.raw IS NOT NULL
            WITH p
        """
    else:
        query += """
        WHERE p.raw IS NOT NULL
        WITH P
        """

    # Adding the other part of query
    query += """
        MATCH (author:DiscourseUser)-[:POSTED]->(p)
        WITH author, p
        OPTIONAL MATCH (u:DiscourseUser)-[:LIKED]->(p)
        WITH author, p, COLLECT(u.username) AS liker_usernames, COLLECT(u.name) AS liker_names
        OPTIONAL MATCH (t:DiscourseTopic {id: p.topicId})
        OPTIONAL MATCH (c:DiscourseCategory)-[:HAS_TOPIC]->(t)
        OPTIONAL MATCH (pr:DiscoursePost)-[:REPLIED_TO]->(p)
        OPTIONAL MATCH (replier_user: DiscourseUser)-[:POSTED]->(pr)
        RETURN
            author.username AS author_username,
            author.name AS author_name,
            t.title AS topic,
            p.id AS postId,
            $forum_endpoint AS forum_endpoint,
            p.raw AS raw,
            p.createdAt AS createdAt,
            p.updatedAt AS updatedAt,
            author.trustLevel AS authorTrustLevel,
            liker_usernames,
            liker_names,
            c.name AS category,
            COLLECT(replier_user.username) AS replier_usernames,
            COLLECT(replier_user.name) AS replier_names
        ORDER BY createdAt
    """
    raw_records, _, _ = neo4j.neo4j_ops.neo4j_driver.execute_query(
        query, from_date=from_date, forum_endpoint=forum_endpoint
    )

    return raw_records


def fetch_raw_posts_grouped(
    forum_endpoint: str, from_date: datetime | None = None
) -> list[neo4j._data.Record]:
    """
    fetch the raw posts from disocurse grouped by date.
    Note: the date is in the format of %Y-%m-%d

    Parameters
    ----------
    forum_endpoint : str
        the forum to extract its data
    from_date : datetime | None
        the posts to retrieve from a specific date
        default is `None` meaning to fetch all posts

    Returns
    ---------
    raw_records_grouped : list[neo4j._data.Record]
        list of neo4j records as the result
    """
    neo4j = Neo4jConnection()

    query = """
    MATCH (p:DiscoursePost {endpoint: $forum_endpoint})
    """

    if from_date:
        query += """
            WHERE
                datetime(p.createdAt) >= datetime($from_date)
                AND p.raw IS NOT NULL
            WITH p
        """
    else:
        query += """
        WHERE p.raw IS NOT NULL
        WITH P
        """

    query += """
        MATCH (author:DiscourseUser)-[:POSTED]->(p)
        WITH author, p
        OPTIONAL MATCH (u:DiscourseUser)-[:LIKED]->(p)
        WITH
            author,
            p,
            COLLECT(u.username) AS liker_usernames,
            COLLECT(u.name) AS liker_names
        OPTIONAL MATCH (t:DiscourseTopic {id: p.topicId})
        OPTIONAL MATCH (c:DiscourseCategory)-[:HAS_TOPIC]->(t)
        OPTIONAL MATCH (pr:DiscoursePost)-[:REPLIED_TO]->(p)
        OPTIONAL MATCH (replier_user: DiscourseUser)-[:POSTED]->(pr)

        WITH author, p, liker_usernames, liker_names,
            t.title AS topic,
            p.id AS postId,
            p.raw AS raw,
            p.createdAt AS createdAt,
            p.updatedAt AS updatedAt,
            author.trustLevel AS authorTrustLevel,
            c.name AS category,
            COLLECT(replier_user.username) AS replier_usernames,
            COLLECT(replier_user.name) AS replier_names,
            // Extract the first 10 characters (date part)
            substring(p.createdAt, 0, 10) AS date

        RETURN
            date,
            COLLECT({
                author_username: author.username,
                author_name: author.name,
                topic: topic,
                postId: postId,
                forum_endpoint: $forum_endpoint,
                raw: raw,
                createdAt: createdAt,
                updatedAt: updatedAt,
                authorTrustLevel: authorTrustLevel,
                liker_usernames: liker_usernames,
                liker_names: liker_names,
                category: category,
                replier_usernames: replier_usernames,
                replier_names: replier_names
            }) AS posts
        ORDER BY date ASC
    """

    raw_records_grouped, _, _ = neo4j.neo4j_ops.neo4j_driver.execute_query(
        query, from_date=from_date, forum_endpoint=forum_endpoint
    )

    return raw_records_grouped
