from datetime import datetime

import neo4j
from github.neo4j_storage.neo4j_connection import Neo4jConnection
from hivemind_etl_helpers.src.db.github.schema import GitHubPullRequest


def fetch_raw_pull_requests(
    repository_id: list[int],
    from_date: datetime | None = None,
) -> list[neo4j._data.Record]:
    """
    fetch pull requests from neo4j data dump

    Parameters
    -----------
    repository_id : list[int]
        a list of repository id to fetch their pull requests
    from_date : datetime | None
        get the pull requests form a specific date that they were created
        defualt is `None`, meaning to apply no filtering on data

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

    if from_date is not None:
        query += "AND datetime(pr.created_at) >= datetime($fromDate)"

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
    """

    def _exec_query(tx, repoIds, from_date):
        result = tx.run(query, repoIds=repoIds, fromDate=from_date)
        return list(result)

    with neo4j_driver.session() as session:
        raw_records = session.execute_read(
            _exec_query,
            repoIds=repository_id,
            from_date=from_date,
        )

    return raw_records


def fetch_pull_requests(
    repository_id: list[int],
    from_date: datetime | None = None,
) -> list[GitHubPullRequest]:
    """
    fetch pull requests from neo4j data dump

    Parameters
    -----------
    repository_id : list[int]
        a list of repository id to fetch their pull requests
    from_date : datetime | None
        get the pull requests form a specific date that they were created
        defualt is `None`, meaning to apply no filtering on data

    Returns
    --------
    github_prs : list[GitHubPullRequest]
        a list of github pull requests extracted from neo4j
    """
    records = fetch_raw_pull_requests(repository_id, from_date)

    github_prs: list[GitHubPullRequest] = []
    for record in records:
        issue = GitHubPullRequest.from_dict(record)
        github_prs.append(issue)

    return github_prs
