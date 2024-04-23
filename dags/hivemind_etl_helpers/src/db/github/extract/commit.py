from datetime import datetime

import neo4j
from github.neo4j_storage.neo4j_connection import Neo4jConnection
from github.neo4j_storage.neo4j_enums import Node, Relationship
from hivemind_etl_helpers.src.db.github.schema import GitHubCommit


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
    query = f"""MATCH (co:{Node.Commit.value})<-[:{Relationship.AUTHORED_BY.value}]-(user:{Node.GitHubUser.value})
        WHERE
        co.repository_id IN $repoIds
    """
    if from_date is not None:
        query += "AND datetime(co.`commit.author.date`) >= datetime($from_date)"

    query += f"""
        OPTIONAL MATCH (co)<-[:{Relationship.COMMITTED_BY.value}]-(user_commiter:{Node.GitHubUser.value})
        OPTIONAL MATCH (co)-[:{Relationship.IS_ON.value}]->(pr:{Node.PullRequest.value})
        MATCH (repo:{Node.Repository.value} {{id: co.repository_id}})
        RETURN
            user.login AS author_name,
            user_commiter.login AS committer_name,
            co.`commit.message` AS message,
            co.`commit.url` AS api_url,
            co.`parents.0.html_url` AS html_url,
            co.repository_id AS repository_id,
            repo.full_name AS repository_name,
            co.sha AS sha,
            co.latestSavedAt AS latest_saved_at,
            co.`commit.author.date` AS created_at,
            co.`commit.verification.reason` AS verification,
            pr.title as related_pr_title
        ORDER BY created_at
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
