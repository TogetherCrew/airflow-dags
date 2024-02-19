from datetime import datetime

import neo4j
from github.neo4j_storage.neo4j_connection import Neo4jConnection
from dags.hivemind_etl_helpers.src.db.github.schema import GitHubCommit


def fetch_raw_commits(
    repository_id: list[int],
    from_date: datetime | None = None,
) -> list[neo4j._data.Record]:
    """
    fetch raw commits from data dump in neo4j

    Parameters
    ------------
    repository_id : list[int]
        a list of repository id to fetch their commits
    from_date : datetime | None
        get the commits form a specific date that they were created
        defualt is `None`, meaning to apply no filtering on data

    Returns
    --------
    raw_records : list[neo4j._data.Record]
        list of neo4j records as the extracted commits
    """
    neo4j_connection = Neo4jConnection()
    neo4j_driver = neo4j_connection.connect_neo4j()
    query = """MATCH (co:Commit)
        WHERE
        co.repository_id IN $repoIds
    """
    if from_date is not None:
        query += "AND datetime(co.`commit.author.date`) >= datetime($from_date)"

    query += """
        MATCH (repo:Repository {id: co.repository_id})
        RETURN
            co.`commit.author.name` as author,
            co.`commit.message` as message,
            co.`commit.url` as api_url,
            co.`parents.0.html_url` as html_url,
            co.repository_id as repository_id,
            repo.full_name as repository_name,
            co.sha as sha,
            co.latestSavedAt as latest_saved_at,
            co.`commit.author.date` as created_at,
            co.`commit.verification.reason` as verification
    """

    def _exec_query(tx, repoIds, from_date):
        result = tx.run(query, repoIds=repoIds, from_date=from_date)
        return list(result)

    with neo4j_driver.session() as session:
        raw_records = session.execute_read(
            _exec_query,
            repoIds=repository_id,
            from_date=from_date,
        )

    return raw_records


def fetch_commits(
    repository_id: list[int],
    from_date: datetime | None = None,
) -> list[GitHubCommit]:
    """
    fetch commits from data dump in neo4j

    Parameters
    ------------
    repository_id : list[int]
        a list of repository id to fetch their commits
    from_date : datetime | None
        get the commits form a specific date that they were created
        defualt is `None`, meaning to apply no filtering on data

    Returns
    --------
    github_commits : list[GitHubCommit]
        list of neo4j records as the extracted commits
    """
    records = fetch_raw_commits(repository_id, from_date)

    github_commits: list[GitHubCommit] = []
    for record in records:
        issue = GitHubCommit.from_dict(record)
        github_commits.append(issue)

    return github_commits
