from datetime import datetime

import neo4j
from github.neo4j_storage.neo4j_connection import Neo4jConnection
from hivemind_etl_helpers.src.db.github.schema import GitHubComment


def fetch_raw_comments(
    repository_id: list[int],
    from_date: datetime | None = None,
    **kwargs,
) -> list[neo4j._data.Record]:
    """
    fetch comments from neo4j data dump

    Parameters
    -----------
    repository_id : list[int]
        a list of repository id to fetch their comments
    from_date : datetime | None
        get the comments form a specific date that they were created
        defualt is `None`, meaning to apply no filtering on data
    **kwargs :
        pr_ids : list[int]
            a list of PullRequest ids to filter data from
        issue_ids : list[int]
            a list of Issue ids to filter data from

    Returns
    --------
    raw_records : list[neo4j._data.Record]
        list of neo4j records as the extracted comments
    """
    neo4j_connection = Neo4jConnection()
    neo4j_driver = neo4j_connection.connect_neo4j()

    pr_ids = kwargs.get("pr_ids", None)
    issue_ids = kwargs.get("issue_ids", None)

    query = """
        MATCH (c:Comment)<-[:CREATED]-(user:GitHubUser)
        MATCH (c)-[:IS_ON]->(info:PullRequest|Issue)
        MATCH (repo:Repository {id: c.repository_id})
        WHERE c.repository_id IN $repoIds
    """

    if from_date is not None:
        query += "AND datetime(c.updated_at) >= datetime($fromDate)"

    # pull request and issue ids
    info_ids: list[int] = []
    if pr_ids:
        info_ids.extend(pr_ids)
    if issue_ids:
        info_ids.extend(issue_ids)

    # if there was some PR and issues to filter
    if len(info_ids) != 0:
        query += f"""
        AND info.id IN $info_ids
        """

    query += """
    RETURN
        user.login as author_name,
        c.id AS id,
        c.created_at AS created_at,
        c.updated_at AS updated_at,
        repo.full_name AS repository_name,
        c.body AS text,
        c.latestSavedAt AS latest_saved_at,
        info.title AS related_title,
        // a comment is always related to one PR or Issue
        LABELS(info)[0] AS related_node,
        c.html_url AS url,
        {
            hooray: c.`reactions.hooray`,
            eyes: c.`reactions.eyes`,
            heart: c.`reactions.heart`,
            laugh: c.`reactions.laugh`,
            confused: c.`reactions.confused`,
            rocket: c.`reactions.rocket`,
            plus1: c.`reactions.+1`,
            minus1: c.`reactions.-1`,
            total_count: c.`reactions.total_count`
        } AS reactions
    ORDER BY datetime(created_at)
    """

    def _exec_query(tx, repoIds, from_date, info_ids):
        result = tx.run(query, repoIds=repoIds, fromDate=from_date, info_ids=info_ids)
        return list(result)

    with neo4j_driver.session() as session:
        raw_records = session.execute_read(
            _exec_query,
            repoIds=repository_id,
            from_date=from_date,
            info_ids=info_ids,
        )

    return raw_records


def fetch_comments(
    repository_id: list[int],
    from_date: datetime | None = None,
    **kwargs,
) -> list[GitHubComment]:
    """
    fetch comments from neo4j data dump

    Parameters
    -----------
    repository_id : list[int]
        a list of repository id to fetch their comments
    from_date : datetime | None
        get the comments form a specific date that they were created
        defualt is `None`, meaning to apply no filtering on data
    **kwargs :
        pr_ids : list[int]
            a list of PullRequest ids to filter data from
        issue_ids : list[int]
            a list of Issue ids to filter data from


    Returns
    --------
    github_comments : list[GitHubPullRequest]
        a list of github comments extracted from neo4j
    """
    records = fetch_raw_comments(repository_id, from_date, **kwargs)

    github_comments: list[GitHubComment] = []
    for record in records:
        comment = GitHubComment.from_dict(record)
        github_comments.append(comment)

    return github_comments
