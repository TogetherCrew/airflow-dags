from enum import Enum


class SummaryType(str, Enum):
    PR = "PullRequest"
    ISSUE = "Issue"
    COMMENT = "Comment"
    COMMIT = "Commit"
