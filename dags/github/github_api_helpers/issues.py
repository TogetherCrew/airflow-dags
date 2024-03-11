import logging

from .smart_proxy import get


def fetch_issue(owner: str, repo: str, issue_number: int):
    """
    Fetches a specific issue from a GitHub repository.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param issue_number: The number of the issue.
    :return: The issue data.
    """
    endpoint = f"https://api.github.com/repos/{owner}/{repo}/issues/{issue_number}"
    response = get(endpoint)
    response_data = response.json()

    logging.info(f"Fetched issue {issue_number} for {owner}/{repo}. Issue: {response_data}")
    return response_data

# Issues

def fetch_issues(owner: str, repo: str, page: int, per_page: int = 100):
    """
    Fetches the issues for a specific repo page by page.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param page: The page number of the results.
    :param per_page: The number of results per page (default is 30).
    :return: A list of issues for the specified repo.
    """
    endpoint = f"https://api.github.com/repos/{owner}/{repo}/issues"

    params = {
        "per_page": per_page,
        "page": page,
        "state": "all",
    }
    response = get(endpoint, params=params)
    response_data = response.json()

    # Filter out pull requests
    issues = [issue for issue in response_data if "pull_request" not in issue]
    is_more_issues = len(response_data) == per_page

    logging.info(
        f"Found {len(issues)} issues for {owner}/{repo} on page {page}. Issues: {issues}"
    )
    return issues, is_more_issues


def get_all_issues(owner: str, repo: str):
    """
    Retrieves all issues for a specific repo.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :return: A list of all issues for the specified repo.
    """
    logging.info(f"Fetching all issues for {owner}/{repo}...")
    all_issues = []
    current_page = 1

    while True:
        logging.info(f"Fetching page {current_page} of issues...")
        issues, is_more_issues = fetch_issues(owner, repo, current_page)
        all_issues.extend(issues)

        if not is_more_issues:
            break  # No more issues to fetch

        current_page += 1

    logging.info(f"Found a total of {len(all_issues)} issues for {owner}/{repo}.")
    return all_issues


# Issue Comments
def fetch_issue_comments(
    owner: str, repo: str, issue_number: int, page: int, per_page: int = 30
):
    """
    Fetches the comments for a specific issue page by page.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param issue_number: The number of the issue.
    :param page: The page number of the results.
    :param per_page: The number of results per page (default is 30).
    :return: A list of comments for the specified issue page.
    """
    endpoint = (
        f"https://api.github.com/repos/{owner}/{repo}/issues/{issue_number}/comments"
    )
    params = {"page": page, "per_page": per_page}
    response = get(endpoint, params=params)
    response_data = response.json()

    logging.info(
        f"Found {len(response_data)} comments for issue {issue_number} on page {page}. Comments: {response_data}"
    )
    return response_data


def get_all_comments_of_issue(owner: str, repo: str, issue_number: int):
    """
    Retrieves all comments for a specific issue.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param issue_number: The number of the issue.
    :return: A list of all comments for the specified issue.
    """
    logging.info(f"Fetching all comments for issue {issue_number}...")
    all_comments = []
    current_page = 1
    while True:
        logging.info(f"Fetching page {current_page} of comments...")
        comments = fetch_issue_comments(owner, repo, issue_number, current_page)
        if not comments:  # Break the loop if no more comments are found
            break
        all_comments.extend(comments)
        current_page += 1

    logging.info(
        f"Found a total of {len(all_comments)} comments for issue {issue_number}."
    )
    return all_comments
