import requests

def fetch_org_repos_page(org_name: str, page: int, per_page: int = 100):
    """
    Fetches the repos for a specific organization in GitHub.

    :param org_name: The name of the organization.
    :param page: The page number of the results.
    :param per_page: The number of results per page (default is 100).
    :return: A list of repos for the specified organization.
    """
    endpoint = f'https://api.github.com/orgs/{org_name}/repos'

    params = {
        "per_page": per_page,
        "page": page
    }
    response = requests.get(endpoint, params=params)
    response_data = response.json()

    return response_data

def get_all_org_repos(org_name: str):
    """
    Retrieves all repos for a specific organization in GitHub.

    :param org_name: The name of the organization.
    :return: A list of repos for the specified organization.
    """
    all_repos = []
    current_page = 1

    while True:
        repos = fetch_org_repos_page(org_name, current_page)

        if not repos:
            break  # No more repositories to fetch

        all_repos.extend(repos)
        current_page += 1

    return all_repos
