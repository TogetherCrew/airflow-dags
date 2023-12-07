from enum import Enum

class Node(Enum):
    GitHubOrganization = "GitHubOrganization"
    GitHubUser = "GitHubUser"
    PullRequest = "PullRequest"
    Repository = "Repository"
    Issue = "Issue"
    Label = "Label"
    Commit = "Commit"
    Comment = "Comment"
    ReviewComment = "ReviewComment"

class Relationship(Enum):
    IS_MEMBER = "IS_MEMBER"
    IS_WITHIN = "IS_WITHIN"
    CREATED = "CREATED"
    ASSIGNED = "ASSIGNED"
    IS_REVIEWER = "IS_REVIEWER"
    HAS_LABEL = "HAS_LABEL"
    COMMITTED = "COMMITTED"
    IS_ON = "IS_ON"
