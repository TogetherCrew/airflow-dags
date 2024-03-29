# flake8: noqa
from .comments import fetch_comments
from .commit import fetch_commits
from .issues import GithubIssueExtraction
from .pull_requests import fetch_pull_requests

class GithubExtraction(GithubIssueExtraction):
    
    def __init__(self):
        pass