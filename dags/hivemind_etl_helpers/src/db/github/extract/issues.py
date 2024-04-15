from datetime import datetime

import neo4j
from github.neo4j_storage.neo4j_connection import Neo4jConnection
from hivemind_etl_helpers.src.db.github.schema import GitHubIssue, GitHubIssueID


class GithubIssueExtraction:
    def __init__(self):
        pass

    def __fetch_raw_issues(
        self,
        repository_id: list[int],
        from_date: datetime | None = None,
    ) -> list[neo4j._data.Record]:
        """
        fetch raw issues from data dump in neo4j

        Parameters
        ------------
        repository_id : list[int]
            a list of repository id to fetch their issues
        from_date : datetime | None
            get the issues form a specific date that they were created
            default is `None`, meaning to apply no filtering on data

        Returns
        --------
        raw_records : list[neo4j._data.Record]
            list of neo4j records as the extracted issues
        """
        neo4j_connection = Neo4jConnection()
        neo4j_driver = neo4j_connection.connect_neo4j()
        query = """MATCH (i:Issue)<-[:CREATED]-(user:GitHubUser)
            WHERE
            i.repository_id IN $repoIds
        """
        if from_date is not None:
            query += "AND datetime(i.updated_at) >= datetime($from_date)"

        query += """
            MATCH (repo:Repository {id: i.repository_id})
            RETURN
                user.login as author_name,
                i.id as id,
                i.title as title,
                i.body as text,
                i.state as state,
                i.state_reason as state_reason,
                i.created_at as created_at,
                i.updated_at as updated_at,
                i.closed_at as closed_at,
                i.latestSavedAt as latest_saved_at,
                i.html_url as url,
                i.repository_id as repository_id,
                repo.full_name as repository_name
            ORDER BY datetime(created_at)
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

    def fetch_issues(
        self,
        repository_id: list[int],
        from_date: datetime | None = None,
    ) -> list[GitHubIssue]:
        """
        fetch issues from data dump in neo4j

        Parameters
        ------------
        repository_id : list[int]
            a list of repository id to fetch their issues
        from_date : datetime | None
            get the issues form a specific date that they were created
            default is `None`, meaning to apply no filtering on data

        Returns
        --------
        github_issues : list[GitHubIssue]
            list of neo4j records as the extracted issues
        """
        records = self.__fetch_raw_issues(repository_id, from_date)

        github_issues: list[GitHubIssue] = []
        for record in records:
            issue = GitHubIssue.from_dict(record)
            github_issues.append(issue)

        return github_issues

    def fetch_issue_ids(
        self,
        repository_id: list[int],
        from_date: datetime | None = None,
    ) -> list[GitHubIssueID]:
        """
        fetch issues from data dump in neo4j

        Parameters
        ------------
        repository_id : list[int]
            a list of repository id to fetch their issues
        from_date : datetime | None
            get the issues form a specific date that they were created
            default is `None`, meaning to apply no filtering on data

        Returns
        --------
        github_issues_ids : list[GitHubIssueID]
            list of neo4j records as the extracted issue ids
        """
        records = self.__fetch_raw_issues(repository_id, from_date)

        github_issue_ids: list[GitHubIssueID] = []
        for record in records:
            issue = GitHubIssueID.from_dict(record)
            github_issue_ids.append(issue)

        return github_issue_ids
