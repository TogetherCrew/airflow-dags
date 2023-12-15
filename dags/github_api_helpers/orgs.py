from .smart_proxy import get


def fetch_org_details(org_name: str):
    """
    Fetches the details of a specific organization in GitHub.

    :param org_name: The name of the organization.
    :return: A dict containing the details of the specified organization.
    """
    endpoint = f"https://api.github.com/orgs/{org_name}"

    response = get(endpoint)
    response_data = response.json()

    return response_data


def fetch_org_members_page(org: str, page: int, per_page: int = 100):
    """
    Fetches a page of members for a specific organization in GitHub.

    :param org: The name of the organization.
    :param page: The page number of the results.
    :param per_page: The number of results per page (default is 100).
    :return: A list of members for the specified organization.
    """
    endpoint = f"https://api.github.com/orgs/{org}/members?role=all"

    params = {"per_page": per_page, "page": page}
    response = get(endpoint, params=params)
    response_data = response.json()

    return response_data


def get_all_org_members(org: str):
    """
    Retrieves all members of a specific organization in GitHub.

    :param org: The name of the organization.
    :return: A list of members of the organization.
    """
    all_members = []
    current_page = 1

    while True:
        members = fetch_org_members_page(org, current_page)

        if not members:
            break  # No more members to fetch

        all_members.extend(members)
        current_page += 1

    return all_members
