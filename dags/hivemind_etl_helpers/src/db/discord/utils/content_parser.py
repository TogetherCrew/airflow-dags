import re


def remove_empty_str(data: list[str]):
    """
    a utility function to remove the empty string from a list

    Parameters
    -----------
    data : list[str]
        a list with string values
    """
    while "" in data:
        data.remove("")

    return data


def check_no_content_only_links(content: str, link_pattern: str = r"\[URL\d\]") -> str:
    """
    check if there's just links in the function and there's no content written

    Parameters
    -----------
    content : str
        the message content
    link_pattern : str
        the pattern of link
        default pattern is for links which is `[URL\d]`

    Returns
    --------
    no_content : bool
        if `True` then there was no content but the links in the given string
    """
    pattern = re.compile(link_pattern)
    replacement = ""

    result_string = re.sub(pattern, replacement, content)

    alphabet_pattern = re.compile(r"[a-zA-Z]")
    no_content = not bool(alphabet_pattern.search(result_string))
    return no_content


def remove_none_from_list(data: list[str | None]) -> list[str]:
    """
    remove the `None` values from a list

    Parameters
    -----------
    data : list[str | None]
        the list of data to process

    Returns
    --------
    data_processed : list[str]
        the data just removed the `None` values
    """
    data_processed = [value for value in data if value is not None]
    return data_processed
