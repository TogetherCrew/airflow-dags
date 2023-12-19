from .smart_proxy import get
import logging


def fetch_repo_labels_page(owner: str, repo: str, page: int, per_page: int = 100):
    """
    Fetches the labels for a specific repository in GitHub.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param page: The page number of the results.
    :param per_page: The number of results per page (default is 100).
    :return: A list of labels for the specified repository.
    """
    endpoint = f"https://api.github.com/repos/{owner}/{repo}/labels"

    params = {"per_page": per_page, "page": page}
    response = get(endpoint, params=params)
    response_data = response.json()

    logging.info(
        f"Found {len(response_data)} labels for {owner}/{repo} on page {page}. Labels: {response_data}"
    )
    return response_data


def get_all_repo_labels(owner: str, repo: str):
    """
    Retrieves all labels for a specific repository in GitHub.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :return: A list of labels for the specified repository.
    """
    logging.info(f"Fetching all labels for {owner}/{repo}...")
    all_labels = []
    current_page = 1

    while True:
        logging.info(f"Fetching page {current_page} of labels...")
        labels = fetch_repo_labels_page(owner, repo, current_page)

        if not labels:
            break  # No more labels to fetch

        all_labels.extend(labels)
        current_page += 1

    logging.info(f"Found a total of {len(all_labels)} labels for {owner}/{repo}.")
    return all_labels
