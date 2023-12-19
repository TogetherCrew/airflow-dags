import logging

from .smart_proxy import get


def extract_pull_request_number_from_review_comment_response(response_data):
    """
    Extracts the pull request number from a review comment response.

    :param response_data: The response data from a review comment request.
    :return: The pull request number.
    """
    pull_request_url = response_data["pull_request_url"]
    pull_request_number = int(pull_request_url.split("/")[-1])

    return pull_request_number


def fetch_repo_review_comments_page(
    owner: str, repo: str, page: int, per_page: int = 100
):
    """
    Fetches the review comments for all pull requests in a GitHub repository.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param page: The page number of the results.
    :param per_page: The number of results per page (default is 100).
    :return: A list of review comments for the specified repository.
    """
    endpoint = f"https://api.github.com/repos/{owner}/{repo}/pulls/comments"

    params = {"per_page": per_page, "page": page}
    response = get(endpoint, params=params)
    response_data = response.json()

    updated_response_data = list(
        map(
            lambda x: {
                **x,
                "pull_request_number": extract_pull_request_number_from_review_comment_response(
                    x
                ),
            },
            response_data,
        )
    )

    msg = f"Found {len(updated_response_data)}"
    msg += f" review comments for {owner}/{repo} on page {page}."
    msg += f" comments: {updated_response_data}"
    logging.info(msg)

    return updated_response_data


def get_all_repo_review_comments(owner: str, repo: str):
    """
    Retrieves all review comments for all pull requests in a GitHub repository.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :return: A list of all review comments for the specified repository.
    """
    logging.info(f"Fetching all comments for {owner}/{repo}...")
    all_comments = []
    current_page = 1

    while True:
        logging.info(f"Fetching page {current_page} of comments...")
        comments = fetch_repo_review_comments_page(owner, repo, current_page)

        if not comments:
            break  # No more comments to fetch

        all_comments.extend(comments)
        current_page += 1

    logging.info(f"Found a total of {len(all_comments)} comments for {owner}/{repo}.")
    return all_comments


def extract_type_from_comment_response(response_data):
    """
    Extracts the type of comment from a comment response.

    :param response_data: The response data from a comment request.
    :return: The type of comment.
    """
    html_url = response_data["html_url"]
    type = html_url.split("/")[-2]
    url = response_data["issue_url"]
    number = url.split("/")[-1]

    if type == "issues":
        return {"type": "issue", "number": number}
    elif type == "pull":
        return {"type": "pull_request", "number": number}
    else:
        return {"number": number}


def fetch_repo_issues_and_prs_comments_page(
    owner: str, repo: str, page: int, per_page: int = 100
):
    """
    Fetches the review comments for all pull requests in a GitHub repository.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :param page: The page number of the results.
    :param per_page: The number of results per page (default is 100).
    :return: A list of review comments for the specified repository.
    """
    endpoint = f"https://api.github.com/repos/{owner}/{repo}/issues/comments"

    params = {"per_page": per_page, "page": page}
    response = get(endpoint, params=params)
    response_data = response.json()

    updated_response_data = list(
        map(lambda x: {**x, **extract_type_from_comment_response(x)}, response_data)
    )

    msg = f"Found {len(updated_response_data)}"
    msg += f" comments for {owner}/{repo} on page {page}."
    msg += f" comments: {updated_response_data}"
    logging.info(msg)

    return updated_response_data


def get_all_repo_issues_and_prs_comments(owner: str, repo: str):
    """
    Retrieves all review comments for all pull requests in a GitHub repository.

    :param owner: The owner of the repository.
    :param repo: The name of the repository.
    :return: A list of all review comments for the specified repository.
    """
    logging.info(f"Fetching all comments for {owner}/{repo}...")
    all_comments = []
    current_page = 1

    while True:
        logging.info(f"Fetching page {current_page} of comments...")
        comments = fetch_repo_issues_and_prs_comments_page(owner, repo, current_page)

        if not comments:
            break  # No more comments to fetch

        all_comments.extend(comments)
        current_page += 1

    logging.info(f"Found a total of {len(all_comments)} comments for {owner}/{repo}.")
    return all_comments
