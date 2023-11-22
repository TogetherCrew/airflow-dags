import requests
def fetch_org_details(org_name: str):
    """
    Fetches the details of a specific organization in GitHub.

    :param org_name: The name of the organization.
    :return: A dict containing the details of the specified organization.
    """
    endpoint = f'https://api.github.com/orgs/{org_name}'

    response = requests.get(endpoint)
    response_data = response.json()

    return response_data
