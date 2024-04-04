# flake8: noqa
from .comments import fetch_comments
from .commit import GithubCommitExtraction
from .issues import fetch_issues
from .pull_requests import fetch_pull_requests


class GithubExtraction(GithubCommitExtraction):
    pass
