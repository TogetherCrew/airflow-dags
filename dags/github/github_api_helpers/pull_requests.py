import logging

from bs4 import BeautifulSoup

from .issues import fetch_issue
from .smart_proxy import get


def fetch_pull_requests(owner: str, repo: str, page: int, per_page: int = 100):
    """
    Fetches the pull requests for a specific repo in a GitHub repository.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param page: The page number of the results.
    :param per_page: The number of results per page (default is 100).
    :return: A list of pull requests for the specified repo.
    """
    endpoint = f"https://api.github.com/repos/{owner}/{repo}/pulls"

    params = {
        "per_page": per_page,
        "page": page,
        "state": "all",
    }
    response = get(endpoint, params=params)
    response_data = response.json()

    logging.info(
        f"Found {len(response_data)} pull requests for {owner}/{repo} on page {page}. Pull requests: {response_data}"
    )
    return response_data


def get_all_pull_requests(owner: str, repo: str):
    """
    Retrieves all pull requests for a specific repo in a GitHub repository.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param pull_number: The number of the pull request.
    :return: A list of all commits for the specified pull request.
    """
    logging.info(f"Fetching all pull requests for {owner}/{repo}...")
    all_pull_requests = []
    current_page = 1

    while True:
        logging.info(f"Fetching page {current_page} of pull requests...")
        pull_requests = fetch_pull_requests(owner, repo, current_page)

        if not pull_requests:
            break  # No more pull requests to fetch

        all_pull_requests.extend(pull_requests)
        current_page += 1

    logging.info(
        f"Found a total of {len(all_pull_requests)} pull requests for {owner}/{repo}."
    )
    return all_pull_requests


def extract_issue_info_from_url(url):
    splitted_url = url.split("/")
    owner = splitted_url[-4]
    repo = splitted_url[-3]
    issue_number = splitted_url[-1]

    return {"owner": owner, "repo": repo, "issue_number": issue_number}


def extract_linked_issues_from_pr(owner: str, repo: str, pull_number: int):
    html_pr_url = f"https://github.com/{owner}/{repo}/pull/{pull_number}"
    response = get(html_pr_url)
    linked_issue = []

    soup = BeautifulSoup(response.text, "html.parser")
    html_linked_issues = soup.find_all(
        "span",
        class_="Truncate truncate-with-responsive-width my-1",
        attrs={"data-view-component": "true"},
    )
    for html_linked_issue in html_linked_issues:
        issue_url = html_linked_issue.find("a").get("href")
        issue_data = extract_issue_info_from_url(issue_url)
        issue_info = fetch_issue(
            issue_data["owner"], issue_data["repo"], issue_data["issue_number"]
        )

        linked_issue.append(issue_info)

    return linked_issue


def fetch_pull_requests_commits(
    owner: str, repo: str, pull_number: int, page: int, per_page: int = 100
):
    """
    Fetches the commits for a specific pull request in a GitHub repository.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param pull_number: The number of the pull request.
    :param page: The page number of the results.
    :param per_page: The number of results per page (default is 100).
    :return: A list of commits for the specified pull request.
    """
    endpoint = (
        f"https://api.github.com/repos/{owner}/{repo}/pulls/{pull_number}/commits"
    )

    params = {"per_page": per_page, "page": page}
    response = get(endpoint, params=params)
    response_data = response.json()

    logging.info(
        f"Found {len(response_data)} commits for pull request {pull_number} on page {page}. Commits: {response_data}"
    )
    return response_data


def get_all_commits_of_pull_request(owner: str, repo: str, pull_number: int):
    """
    Retrieves all commits for a specific pull request in a GitHub repository.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param pull_number: The number of the pull request.
    :return: A list of all commits for the specified pull request.
    """
    logging.info(f"Fetching all commits for pull request {pull_number}...")
    all_commits = []
    current_page = 1

    while True:
        logging.info(f"Fetching page {current_page} of commits...")
        commits = fetch_pull_requests_commits(owner, repo, pull_number, current_page)

        if not commits:
            break  # No more commits to fetch

        all_commits.extend(commits)
        current_page += 1

    logging.info(
        f"Found a total of {len(all_commits)} commits for pull request {pull_number}."
    )
    return all_commits


def fetch_pull_request_comments(
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
        f"Found {len(response_data)} comments for pull request {issue_number} on page {page}. Comments: {response_data}"
    )
    return response_data


def get_all_comments_of_pull_request(owner: str, repo: str, issue_number: int):
    """
    Retrieves all comments for a specific issue.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param issue_number: The number of the issue.
    :return: A list of all comments for the specified issue.
    """
    logging.info(f"Fetching all comments for pull request {issue_number}...")
    all_comments = []
    current_page = 1
    while True:
        logging.info(f"Fetching page {current_page} of comments...")
        comments = fetch_pull_request_comments(owner, repo, issue_number, current_page)
        if not comments:  # Break the loop if no more comments are found
            break
        all_comments.extend(comments)
        current_page += 1

    logging.info(
        f"Found a total of {len(all_comments)} comments for pull request {issue_number}."
    )
    return all_comments


def fetch_pull_request_review_comments(
    owner: str, repo: str, pull_number: int, page: int, per_page: int = 100
):
    """
    Fetches the review comments for a specific pull request page by page.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param pull_number: The number of the pull request.
    :param page: The page number of the results.
    :param per_page: The number of results per page (default is 30).
    :return: A list of review comments for the specified pull request page.
    """
    endpoint = (
        f"https://api.github.com/repos/{owner}/{repo}/pulls/{pull_number}/comments"
    )
    params = {"page": page, "per_page": per_page}
    response = get(endpoint, params=params)
    response_data = response.json()

    msg = f"Found {len(response_data)} review comments"
    msg += f"for pull request {pull_number} on page {page}."
    msg += f"Comments: {response_data}"
    logging.info(msg)

    return response_data


def get_all_review_comments_of_pull_request(owner: str, repo: str, pull_number: int):
    """
    Retrieves all review comments for a specific pull request.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param pull_number: The number of the pull request.
    :return: A list of all review comments for the specified pull request.
    """
    logging.info(f"Fetching all review comments for pull request {pull_number}...")
    all_comments = []
    current_page = 1
    while True:
        logging.info(f"Fetching page {current_page} of review comments...")
        comments = fetch_pull_request_review_comments(
            owner, repo, pull_number, current_page
        )
        if not comments:  # Break the loop if no more comments are found
            break
        all_comments.extend(comments)
        current_page += 1

    logging.info(
        f"Found a total of {len(all_comments)} review comments for pull request {pull_number}."
    )
    return all_comments


def fetch_review_comment_reactions(
    owner: str, repo: str, comment_id: int, page: int, per_page: int = 100
):
    """
    Fetches the reactions for a specific pull request comment.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param comment_id: The ID of the comment.
    :param page: The page number of the results.
    :param per_page: The number of results per page (default is 100).
    :return: A list of reactions for the specified pull request comment.
    """
    endpoint = f"https://api.github.com/repos/{owner}/{repo}/pulls/comments/{comment_id}/reactions"
    params = {"page": page, "per_page": per_page}
    response = get(endpoint, params=params)
    response_data = response.json()

    msg = f"Found {len(response_data)} reactions"
    msg += f"for review comment {comment_id} on page {page}."
    msg += f"Reactions: {response_data}"
    logging.info(msg)

    return response_data


def get_all_reactions_of_review_comment(owner: str, repo: str, comment_id: int):
    """
    Retrieves all reactions for a specific pull request comment.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param comment_id: The ID of the comment.
    :return: A list of all reactions for the specified pull request comment.
    """
    logging.info(f"Fetching all reactions for review comment {comment_id}...")
    all_reactions = []
    current_page = 1
    while True:
        logging.info(f"Fetching page {current_page} of reactions...")
        reactions = fetch_comment_reactions(owner, repo, comment_id, current_page)
        if not reactions:  # Break the loop if no more reactions are found
            break
        all_reactions.extend(reactions)
        current_page += 1

    logging.info(
        f"Found a total of {len(all_reactions)} reactions for review comment {comment_id}."
    )
    return all_reactions


def fetch_comment_reactions(
    owner: str, repo: str, comment_id: int, page: int, per_page: int = 100
):
    """
    Fetches the reactions for a specific issue comment.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param comment_id: The ID of the comment.
    :param page: The page number of the results.
    :param per_page: The number of results per page (default is 100).
    :return: A list of reactions for the specified issue comment.
    """
    endpoint = f"https://api.github.com/repos/{owner}/{repo}/issues/comments/{comment_id}/reactions"
    params = {"page": page, "per_page": per_page}
    response = get(endpoint, params=params)
    response_data = response.json()

    logging.info(
        f"Found {len(response_data)} reactions for comment {comment_id} on page {page}. Reactions: {response_data}"
    )
    return response_data


def get_all_reactions_of_comment(owner: str, repo: str, comment_id: int):
    """
    Retrieves all reactions for a specific issue comment.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param comment_id: The ID of the comment.
    :return: A list of all reactions for the specified issue comment.
    """
    logging.info(f"Fetching all reactions for comment {comment_id}...")
    all_reactions = []
    current_page = 1
    while True:
        logging.info(f"Fetching page {current_page} of reactions...")
        reactions = fetch_comment_reactions(owner, repo, comment_id, current_page)
        if not reactions:  # Break the loop if no more reactions are found
            break
        all_reactions.extend(reactions)
        current_page += 1

    logging.info(
        f"Found a total of {len(all_reactions)} reactions for comment {comment_id}."
    )
    return all_reactions


def fetch_pull_request_reviews(
    owner: str, repo: str, pull_number: int, page: int, per_page: int = 100
):
    """
    Fetches the reviews for a specific pull request page by page.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param pull_number: The number of the pull request.
    :param page: The page number of the results.
    :param per_page: The number of results per page (default is 100).
    :return: A list of reviews for the specified pull request page.
    """
    endpoint = (
        f"https://api.github.com/repos/{owner}/{repo}/pulls/{pull_number}/reviews"
    )
    params = {"page": page, "per_page": per_page}
    response = get(endpoint, params=params)
    response_data = response.json()

    logging.info(
        f"Found {len(response_data)} reviews for pull request {pull_number} on page {page}. Reviews: {response_data}"
    )
    return response_data


def get_all_reviews_of_pull_request(owner: str, repo: str, pull_number: int):
    """
    Retrieves all reviews for a specific pull request.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param pull_number: The number of the pull request.
    :return: A list of all reviews for the specified pull request.
    """
    logging.info(f"Fetching all reviews for pull request {pull_number}...")
    all_reviews = []
    current_page = 1
    while True:
        logging.info(f"Fetching page {current_page} of reviews...")
        reviews = fetch_pull_request_reviews(owner, repo, pull_number, current_page)
        if not reviews:  # Break the loop if no more reviews are found
            break
        all_reviews.extend(reviews)
        current_page += 1

    logging.info(
        f"Found a total of {len(all_reviews)} reviews for pull request {pull_number}."
    )
    return all_reviews


def fetch_pull_request_files_page(
    owner: str, repo: str, pull_number: int, page: int, per_page: int = 100
):
    """
    Fetches the files of a specific pull request in a GitHub repository.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param pull_number: The number of the pull request.
    :param page: The page number of the results.
    :param per_page: The number of results per page (default is 30).
    :return: A list of files for the specified pull request.
    """
    endpoint = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pull_number}/files"

    params = {"per_page": per_page, "page": page}
    response = get(endpoint, params=params)
    response_data = response.json()

    logging.info(
        f"Found {len(response_data)} files for pull request {pull_number} on page {page}. Files: {response_data}"
    )
    return response_data


def get_all_pull_request_files(owner: str, repo: str, pull_number: int):
    """
    Retrieves all files for a specified pull request in a GitHub repository.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param pull_number: The number of the pull request.
    :return: A list of all files for the specified pull request.
    """
    logging.info(f"Fetching all files for pull request {pull_number}...")
    files = []
    page = 1
    while True:
        logging.info(f"Fetching page {page} of files...")
        page_files = fetch_pull_request_files_page(owner, repo, pull_number, page)
        if not page_files:
            break
        files.extend(page_files)
        page += 1

    logging.info(f"Found a total of {len(files)} files for pull request {pull_number}.")
    return files
