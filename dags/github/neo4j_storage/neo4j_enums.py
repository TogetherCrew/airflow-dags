from enum import Enum


class Node(Enum):
    # This node is created by the API, and we receive a list of organizations detail to extract data from
    prefix = "GitHub"
    OrganizationProfile = f"{prefix}OrganizationProfile"
    GitHubOrganization = f"{prefix}Organization"
    GitHubUser = f"{prefix}User"
    PullRequest = f"{prefix}PullRequest"
    Repository = f"{prefix}Repository"
    Issue = f"{prefix}Issue"
    Label = f"{prefix}Label"
    Commit = f"{prefix}Commit"
    Comment = f"{prefix}Comment"
    ReviewComment = f"{prefix}ReviewComment"
    File = f"{prefix}File"


class Relationship(Enum):
    IS_MEMBER = "IS_MEMBER"
    IS_WITHIN = "IS_WITHIN"
    CREATED = "CREATED"
    ASSIGNED = "ASSIGNED"
    IS_REVIEWER = "IS_REVIEWER"
    REVIEWED = "REVIEWED"
    HAS_LABEL = "HAS_LABEL"
    COMMITTED_BY = "COMMITTED_BY"
    AUTHORED_BY = "AUTHORED_BY"
    IS_ON = "IS_ON"
    CHANGED = "CHANGED"
    REACTED = "REACTED"
    LINKED = "LINKED"
