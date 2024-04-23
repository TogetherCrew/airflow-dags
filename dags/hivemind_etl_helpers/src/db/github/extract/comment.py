from datetime import datetime

import neo4j
from github.neo4j_storage.neo4j_connection import Neo4jConnection
from github.neo4j_storage.neo4j_enums import Node, Relationship
from hivemind_etl_helpers.src.db.github.schema import GitHubComment


class GitHubCommentExtraction:
    def __init__(self):
        """
        Initializes the GitHubCommentExtraction class
        without requiring any parameters.
        Establishes a connection to the Neo4j database.
        """
        self.neo4j_connection = Neo4jConnection()
        self.neo4j_driver = self.neo4j_connection.connect_neo4j()

    def _fetch_raw_comments(
        self, repository_id: list[int], from_date: datetime | None = None, **kwargs
    ) -> list[neo4j.Record]:
        """
        Fetch comments from neo4j data dump based on
        provided repository IDs and an optional date.

        Parameters
        ----------
        repository_id : list[int]
            A list of repository IDs to fetch comments for.
        from_date : datetime | None, optional
            A specific date from which to start fetching comments.
            If None, no date filtering is applied.

        Returns
        -------
        list[neo4j.Record]
            A list of neo4j records as the extracted comments.
        """
        pr_ids = kwargs.get("pr_ids", None)
        issue_ids = kwargs.get("issue_ids", None)

        query = f"""
            MATCH (c:{Node.Comment.value})<-[:{Relationship.CREATED.value}]-(user:{Node.GitHubUser.value})
            MATCH (c)-[:{Relationship.IS_ON.value}]->(info:{Node.PullRequest.value}|{Node.Issue.value})
            MATCH (repo:{Node.Repository.value} {{id: c.repository_id}})
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
            query += "AND info.id IN $info_ids"

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
            result = tx.run(
                query, repoIds=repoIds, fromDate=from_date, info_ids=info_ids
            )
            return list(result)

        with self.neo4j_driver.session() as session:
            raw_records = session.execute_read(
                _exec_query,
                repoIds=repository_id,
                from_date=from_date,
                info_ids=info_ids,
            )
        return raw_records

    def fetch_comments(
        self, repository_id: list[int], from_date: datetime | None = None, **kwargs
    ) -> list[GitHubComment]:
        """
        Fetch comments from neo4j data dump and convert them to GitHubComment
        objects based on provided parameters.

        Parameters
        ----------
        repository_id : list[int]
            A list of repository IDs to fetch comments for.
        from_date : datetime | None, optional
            A specific date from which to start fetching comments.
            If None, no date filtering is applied.
        **kwargs :
            Additional keyword arguments such as: `
            pr_ids` and `issue_ids` for filtering.

        Returns
        -------
        list[GitHubComment]
            A list of GitHubComment objects.
        """

        records = self._fetch_raw_comments(repository_id, from_date, **kwargs)

        github_comments: list[GitHubComment] = []
        for record in records:
            comment = GitHubComment.from_dict(record)
            github_comments.append(comment)
        return github_comments
