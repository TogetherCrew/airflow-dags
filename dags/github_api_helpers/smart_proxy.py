import random

import requests


def get(url: str, params=None):
    """
    Sends a GET request With Smart Proxy.

    :param url: URL for the new :class:`Request` object.
    :param params: (optional) Dictionary, list of tuples or bytes to send
        in the query string for the :class:`Request`.
    :param **kwargs: Optional arguments that ``request`` takes.
    :return: :class:`Response <Response>` object
    :rtype: requests.Response
    """

    random_port = random.randint(20001, 29980)
    proxy_url = f"http://spusfxy185:TwinTwinTwin@eu.dc.smartproxy.com:{random_port}"
    print("proxy_url: ", proxy_url)
    proxies = {
        "http": proxy_url,
        "https": proxy_url,
    }
    return requests.get(url=url, params=params, proxies=proxies)
