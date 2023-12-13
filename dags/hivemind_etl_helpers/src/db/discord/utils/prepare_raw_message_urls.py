from urllib.parse import urlparse
from uuid import uuid1


def extract_urls(text: str) -> list[str]:
    """
    extract the urls within the text and just return the urls

    Parameters
    ------------
    text : str
        the raw text

    Returns
    ---------
    urls : list[str]
        the list of urls within the text
    """
    urls = []
    words = text.split()
    for word in words:
        parsed_url = urlparse(word)
        if parsed_url.scheme and parsed_url.netloc:
            urls.append(parsed_url.scheme + "://" + parsed_url.netloc + parsed_url.path)
    return urls


def prepare_raw_message_urls(message: str) -> tuple[str, dict[str, str]]:
    """
    process the message urls such that URLs are substituted with a reference, for example: “URL87uuhIo”.
    Adding uuid after the URL charater

    Parameters
    -----------
    message : str
        the message raw string

    Returns
    --------
    prepared_msg : str
        the message with its urls are updated to a reference
    references : dict[str, str]
        the url reference dict that keys are reference name
        and values are the actual url
    """
    msg_urls = extract_urls(message)

    references: dict[str, str] = {}

    # initializing
    prepared_msg = message

    for idx, url in enumerate(msg_urls):
        url_reference = f"[URL{idx}]"
        prepared_msg = prepared_msg.replace(url, url_reference)
        references[url_reference] = url

    return prepared_msg, references
