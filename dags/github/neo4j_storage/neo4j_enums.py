from enum import Enum


class Node(Enum):
    # This node is created by the API, and we receive a list of organizations detail to extract data from
    OrganizationProfile = "OrganizationProfile"
    GitHubOrganization = "GitHubOrganization"
    GitHubUser = "GitHubUser"
    PullRequest = "PullRequest"
    Repository = "Repository"
    Issue = "Issue"
    Label = "Label"
    Commit = "Commit"
    Comment = "Comment"
    ReviewComment = "ReviewComment"
    File = "File"


class Relationship(Enum):
    IS_MEMBER = "IS_MEMBER"
    IS_WITHIN = "IS_WITHIN"
    CREATED = "CREATED"
    ASSIGNED = "ASSIGNED"
    IS_REVIEWER = "IS_REVIEWER"
    REVIEWED = "REVIEWED"
    HAS_LABEL = "HAS_LABEL"
    COMMITTED = "COMMITTED"
    IS_ON = "IS_ON"
    CHANGED = "CHANGED"
