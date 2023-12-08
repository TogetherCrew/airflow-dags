from enum import Enum

class Node(Enum):
    GitHubOrganization = "GitHubOrganization"
    GitHubUser = "GitHubUser"
    PullRequest = "PullRequest"
    Repository = "Repository"
    Issue = "Issue"


class Relationship(Enum):
    IS_MEMBER = "IS_MEMBER"
    IS_WITHIN = "IS_WITHIN"
    CREATED = "CREATED"
