from enum import Enum

class Node(Enum):
    GitHubOrganization = "GitHubOrganization"
    GitHubUser = "GitHubUser"
    PullRequest = "PullRequest"
    Repository = "Repository"


class Relationship(Enum):
    IS_MEMBER = "IS_MEMBER"
    IS_WITHIN = "IS_WITHIN"
