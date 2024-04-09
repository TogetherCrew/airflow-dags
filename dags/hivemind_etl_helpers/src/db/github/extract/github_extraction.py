from datetime import datetime

from dags.hivemind_etl_helpers.src.db.github.extract import GitHubCommentExtraction
from dags.hivemind_etl_helpers.src.db.github.schema.comment import GitHubComment

# Note
# Composition might fit here better then inheritance:
# 1. The Extraction class is more about orchestrating these different
#  data-fetching operations rather than being a specialized version of them,
#  which suggests a "has-a" relationship more than an "is-a" relationship.
# 2. Avoidance of Multiple Inheritance
# 3. Flexibillity of the Extraction class
# 4. Encapsulation and Modularity

# it's essential to evaluate whether the relationship between our classes
# truly fits the "is-a" model.
# In our case, composition not only
# avoids the complexities associated with multiple inheritance
# but also promotes better design principles by keeping our classes focused,
# loosely coupled, and easier to maintain.


# Without dependency injection


class GithubExtraction:
    def __init__(self):
        self.comment_extraction = GitHubCommentExtraction()
        # to be uncommented once other pull requests
        # regarding `extraction` are ready
        # self.commits_extraction = GithubCommitExtraction()
        # self.issues_extraction = GithubIssuesExtraction()
        # self.pull_requests_extraction = GithubPullRequestsExtraction()

    def fetch_comments(
        self, repository_id: list[int], from_date: datetime | None = None, **kwargs
    ) -> list[GitHubComment]:
        return self.comment_extraction.fetch_comments(
            repository_id, from_date, **kwargs
        )


# With dependency injection

# class GithubExtraction:
#     def __init__(self, comment_extraction, issues_extraction,
#                  pull_requests_extraction):
#         # Instead of creating instances, we accept them as arguments
#         self.comment_extraction = comment_extraction
#         self.issues_extraction = issues_extraction
#         self.pull_requests_extraction = pull_requests_extraction

#     def fetch_all_data(self):
#         comments_data = self.comment_extraction.fetch_comments()
#         issues_data = self.issues_extraction.fetch_issues()
#         pull_requests_data = (
#             self.pull_requests_extraction
#             .fetch_pull_requests()
#         )
#         # Process the fetched data as needed
