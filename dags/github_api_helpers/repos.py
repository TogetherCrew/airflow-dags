import logging

from .smart_proxy import get


def fetch_org_repos_page(org_name: str, page: int, per_page: int = 100):
    """
    Fetches the repos for a specific organization in GitHub.

    :param org_name: The name of the organization.
    :param page: The page number of the results.
    :param per_page: The number of results per page (default is 100).
    :return: A list of repos for the specified organization.
    """
    endpoint = f"https://api.github.com/orgs/{org_name}/repos"

    params = {"per_page": per_page, "page": page}
    response = get(endpoint, params=params)
    response_data = response.json()

    logging.info(
        f"Found {len(response_data)} repos for organization {org_name} on page {page}. Repos: {response_data}"
    )
    return response_data


def get_all_org_repos(org_name: str):
    """
    Retrieves all repos for a specific organization in GitHub.

    :param org_name: The name of the organization.
    :return: A list of repos for the specified organization.
    """
    logging.info(f"Fetching all repos for organization {org_name}...")
    all_repos = []
    current_page = 1

    while True:
        logging.info(f"Fetching page {current_page} of repos...")
        repos = fetch_org_repos_page(org_name, current_page)

        if not repos:
            break  # No more repositories to fetch

        all_repos.extend(repos)
        current_page += 1

    logging.info(
        f"Found a total of {len(all_repos)} repos for organization {org_name}."
    )
    return all_repos


def fetch_repo_contributors_page(owner: str, repo: str, page: int, per_page: int = 100):
    """
    Fetches the contributors for a specific repository in GitHub.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param page: The page number of the results.
    :param per_page: The number of results per page (default is 100).
    :return: A list of contributors for the specified repository.
    """
    endpoint = f"https://api.github.com/repos/{owner}/{repo}/contributors"

    params = {"per_page": per_page, "page": page}
    response = get(endpoint, params=params)
    response_data = response.json()

    logging.info(
        f"Found {len(response_data)} contributors for {owner}/{repo} on page {page}. Contributors: {response_data}"
    )
    return response_data


def get_all_repo_contributors(owner: str, repo: str):
    """
    Retrieves all contributors for a specific repository in GitHub.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :return: A list of contributors for the specified repository.
    """
    logging.info(f"Fetching all contributors for {owner}/{repo}...")
    all_contributors = []
    current_page = 1

    while True:
        logging.info(f"Fetching page {current_page} of contributors...")
        contributors = fetch_repo_contributors_page(owner, repo, current_page)

        if not contributors:
            break  # No more contributors to fetch

        all_contributors.extend(contributors)
        current_page += 1

    logging.info(
        f"Found a total of {len(all_contributors)} contributors for {owner}/{repo}."
    )
    return all_contributors
