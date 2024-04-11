import logging

from .smart_proxy import get


def fetch_commits(owner: str, repo: str, page: int, per_page: int = 100):
    """
    Fetches the commits for a specific repo page by page.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param page: The page number of the results.
    :param per_page: The number of results per page (default is 30).
    :return: A list of commits for the specified repo.
    """
    endpoint = f"https://api.github.com/repos/{owner}/{repo}/commits"

    params = {"per_page": per_page, "page": page}
    response = get(endpoint, params=params)
    response_data = response.json()

    logging.info(
        f"Found {len(response_data)} commits for {owner}/{repo} on page {page}. Commits: {response_data}"
    )
    return response_data


def get_all_commits(owner: str, repo: str):
    """
    Retrieves all commits for a specific repo.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :return: A list of all commits for the specified repo.
    """
    logging.info(f"Fetching all commits for {owner}/{repo}...")
    all_commits = []
    current_page = 1

    while True:
        logging.info(f"Fetching page {current_page} of commits...")
        commits = fetch_commits(owner, repo, current_page)

        if not commits:
            break  # No more commits to fetch

        all_commits.extend(commits)
        current_page += 1

    logging.info(f"Found a total of {len(all_commits)} commits for {owner}/{repo}.")
    return all_commits


def fetch_commit_details(owner: str, repo: str, commit_sha: str):
    """
    Fetches detailed information about a specific commit.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param commit_sha: The SHA hash of the commit.
    :return: Detailed information about the specified commit.
    """
    endpoint = f"https://api.github.com/repos/{owner}/{repo}/commits/{commit_sha}"
    response = get(endpoint)
    response_data = response.json()

    logging.info(
        f"Found details for commit {commit_sha} of {owner}/{repo}: {response_data}"
    )
    return response_data


def fetch_commit_files(owner: str, repo: str, sha: str):
    """
    Retrieves the files changed in a specific commit of a GitHub repository.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param sha: The SHA identifier of the commit.
    :return: A list of files changed in the specified commit.
    """
    logging.info(f"Fetching files changed in commit {sha} of {owner}/{repo}...")
    commit_details = fetch_commit_details(owner, repo, sha)
    if "files" in commit_details:
        logging.info(
            f"Found {len(commit_details['files'])} files changed in commit {sha} of {owner}/{repo}."
        )
        return commit_details["files"]
    else:
        logging.info(f"No files changed in commit {sha} of {owner}/{repo}.")
        return []


def fetch_commit_pull_requests(owner: str, repo: str, sha: str) -> list:
    """
    fetch the pull requests for a specific comment of GitHub repository

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param sha: The SHA identifier of the commit.
    :return: A list of pull requests associated with the specified commit.
    """
    logging.info(f"Fetching pull requests of {owner}/{repo}/commits/{sha}")
    endpoint = f"https://api.github.com/repos/{owner}/{repo}/commits/{sha}/pulls"
    response = get(endpoint)
    response_data = response.json()

    return response_data
