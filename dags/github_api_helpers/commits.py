import requests

def fetch_commits(owner: str, repo: str, page: int, per_page: int = 100):
    """
    Fetches the commits for a specific repo page by page.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param page: The page number of the results.
    :param per_page: The number of results per page (default is 30).
    :return: A list of commits for the specified repo.
    """
    endpoint = f'https://api.github.com/repos/{owner}/{repo}/commits'

    params = {
        "per_page": per_page,
        "page": page
    }
    response = requests.get(endpoint, params=params)
    response_data = response.json()

    return response_data

def get_all_commits(owner: str, repo: str):
    """
    Retrieves all commits for a specific repo.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :return: A list of all commits for the specified repo.
    """
    all_commits = []
    current_page = 1

    while True:
        commits = fetch_commits(owner, repo, current_page)

        if not commits:
            break  # No more commits to fetch

        all_commits.extend(commits)
        current_page += 1

    return all_commits

def fetch_commit_details(owner: str, repo: str, commit_sha: str):
    """
    Fetches detailed information about a specific commit.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param commit_sha: The SHA hash of the commit.
    :return: Detailed information about the specified commit.
    """
    endpoint = f'https://api.github.com/repos/{owner}/{repo}/commits/{commit_sha}'
    response = requests.get(endpoint)
    return response.json()
