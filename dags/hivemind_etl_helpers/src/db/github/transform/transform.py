from hivemind_etl_helpers.src.db.github.schema import (
    GitHubComment,
    GitHubCommit,
    GitHubIssue,
    GitHubPullRequest,
)
from llama_index.core import Document

from .comments import transform_comments
from .commits import transform_commits
from .pull_requests import transform_prs
from .issues import (
    transform_comment_of_issue,
    transform_issues,
)


class GitHubTransformation:
    def transform_comments(self, data: list[GitHubComment]) -> list[Document]:
        """
        transform the github comments data to a list of llama_index documents

        Parameters
        -----------
        data : list[GitHubComment]
            a list of github comments raw data

        Returns
        ---------
        transformed_comments : list[llama_index.Document]
            a list of llama index documents to be saved
        """
        transformed_comments = transform_comments(data)
        return transformed_comments

    def transform_commits(self, data: list[GitHubCommit]) -> list[Document]:
        """
        transform the github commits data to a list of llama_index documents

        Parameters
        -----------
        data : list[GitHubCommit]
            a list of github commits raw data

        Returns
        ---------
        transformed_commits : list[llama_index.Document]
            a list of llama index documents to be saved
        """
        transformed_commits = transform_commits(data)
        return transformed_commits

    def transform_issues(
        self, data: list[GitHubIssue]
    ) -> tuple[list[Document], list[Document]]:
        """
        transform the github issues data to a list of llama_index documents

        Parameters
        -----------
        data : list[GitHubIssue]
            a list of github issues raw data

        Returns
        ---------
        transformed_issues : list[llama_index.Document]
            a list of llama index documents to be saved
        transformed_issue_comments : list[llama_index.Document]
            the first comment of issues
        """
        transformed_issues, transformed_issue_comments = transform_issues(data)
        return transformed_issues, transformed_issue_comments

    def transform_comment_of_issue(self, data: GitHubIssue) -> Document:
        """
        the first comment of a issue can be long, so it might not be fitted
        into metadata (avoiding metadata longer than chunk size error).
        So we're preparing the comment into another document

        Parameters
        ------------
        data : GitHubIssue
            the related github issue having the first comment within it

        Returns
        ---------
        document : llama_index.Document
            the comment document within the github issue
        """
        document = transform_comment_of_issue(data)
        return document

    def transform_pull_requests(self, data: list[GitHubPullRequest]) -> list[Document]:
        """
        transform the github pull request data to a list of llama_index documents

        Parameters
        -----------
        data : list[GitHubPullRequest]
            a list of github pull request raw data

        Returns
        ---------
        transformed_prs : list[llama_index.Document]
            a list of llama index documents to be saved
        """
        transformed_prs = transform_prs(data)
        return transformed_prs
