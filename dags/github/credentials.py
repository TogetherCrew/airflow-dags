import os

from dotenv import load_dotenv


def load_smart_proxy_url() -> str:
    """
    load smart proxy credentials and return the url

    Returns
    --------
    url : str
        the smart proxy url made by credentials
    """
    load_dotenv()

    protocol = os.getenv("SMART_PROXY_PROTOCOL")
    user = os.getenv("SMART_PROXY_USER")
    password = os.getenv("SMART_PROXY_PASSWORD")
    host = os.getenv("SMART_PROXY_HOST")

    if user is None or password is None or host is None or protocol is None:
        raise EnvironmentError("One of the smart proxy env variables was not set!")

    url = f"{protocol}://{user}:{password}@{host}"

    return url
