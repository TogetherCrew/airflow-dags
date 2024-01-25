from urlextract import URLExtract


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
    msg_urls = URLExtract().find_urls(message)

    references: dict[str, str] = {}

    # initializing
    prepared_msg = message

    for idx, url in enumerate(msg_urls):
        url_reference = f"[URL{idx}]"
        prepared_msg = prepared_msg.replace(url, url_reference)
        references[url_reference] = url

    return prepared_msg, references
