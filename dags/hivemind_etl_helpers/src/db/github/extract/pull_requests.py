from datetime import datetime

import neo4j
from github.neo4j_storage.neo4j_connection import Neo4jConnection
from hivemind_etl_helpers.src.db.github.schema import GitHubPullRequest


def fetch_raw_pull_requests(
    repository_id: list[int],
    from_date_updated: datetime | None = None,
    from_date_created: datetime | None = None,
) -> list[neo4j._data.Record]:
    """
    fetch pull requests from neo4j data dump

    Parameters
    -----------
    repository_id : list[int]
        a list of repository id to fetch their pull requests
    from_date_updated : datetime | None
        get the pull requests form a specific date that
        they were updated (`closed_at` or `merged_at`)
        defualt is `None`, meaning to apply no filtering on updates.
    from_date_created : datetime | None
        get the pull requests from a spcific date that
        they were created (`created_at`)
        defualt is `None`, meaning to apply no filtering on created at

    Notes: The reason we're separating the from dates in prs is because
    in case of no updates applied (closed_at or merged_at gets filled), the update
    files would be null.

    Returns
    --------
    raw_records : list[neo4j._data.Record]
        list of neo4j records as the extracted pull requests
    """
    neo4j_connection = Neo4jConnection()
    neo4j_driver = neo4j_connection.connect_neo4j()

    # TODO: Update query when `Issue` relation was made.
    # We would need to add the issues related to a PR
    query = """
        MATCH (pr:PullRequest)<-[:CREATED]-(user:GitHubUser)
        MATCH (repo:Repository {id: pr.repository_id})
        WHERE
            pr.repository_id IN $repoIds
    """
    if from_date_created:
        query += "AND datetime(pr.created_at) >= datetime($fromDateCreated)"

    if from_date_updated is not None:
        query += "AND ( datetime(pr.merged_at) >= datetime($fromDateUpdated) OR "
        query += "datetime(pr.closed_at) >= datetime($fromDateUpdated) OR"
        query += "(pr.merged_at IS NULL OR pr.closed_at IS NULL))"

    query += """
    RETURN
        user.login as author_name,
        pr.repository_id as repository_id,
        repo.full_name as repository_name,
        pr.issue_url as issue_url,
        pr.created_at as created_at,
        pr.title as title,
        pr.id as id,
        pr.closed_at as closed_at,
        pr.merged_at as merged_at,
        pr.state as state,
        pr.html_url as url,
        pr.latestSavedAt as latest_saved_at
    ORDER BY datetime(created_at)
    """

    def _exec_query(tx, repoIds, from_date_updated, from_date_created):
        result = tx.run(
            query,
            repoIds=repoIds,
            fromDateUpdated=from_date_updated,
            fromDateCreated=from_date_created,
        )
        return list(result)

    with neo4j_driver.session() as session:
        raw_records = session.execute_read(
            _exec_query,
            repoIds=repository_id,
            from_date_updated=from_date_updated,
            from_date_created=from_date_created,
        )

    return raw_records


def fetch_pull_requests(
    repository_id: list[int],
    from_date_updated: datetime | None = None,
    from_date_created: datetime | None = None,
) -> list[GitHubPullRequest]:
    """
    fetch pull requests from neo4j data dump

    Parameters
    -----------
    repository_id : list[int]
        a list of repository id to fetch their pull requests
    from_date_updated : datetime | None
        get the pull requests form a specific date that they were created
        defualt is `None`, meaning to apply no filtering on data
    from_date_created : datetime | None
        get the pull requests from a spcific date that
        they were created (`created_at`)
        defualt is `None`, meaning to apply no filtering on created at

    Notes: The reason we're separating the from dates in prs is because
    in case of no updates applied (closed_at or merged_at gets filled), the update
    files would be null.

    Returns
    --------
    github_prs : list[GitHubPullRequest]
        a list of github pull requests extracted from neo4j
    """
    records = fetch_raw_pull_requests(
        repository_id, from_date_updated, from_date_created
    )

    github_prs: list[GitHubPullRequest] = []
    for record in records:
        issue = GitHubPullRequest.from_dict(record)
        github_prs.append(issue)

    return github_prs
