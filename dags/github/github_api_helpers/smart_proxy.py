import logging
import random

import requests
from requests import Response


class UniqueRandomNumbers:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(UniqueRandomNumbers, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        self.original_numbers = list(range(20001, 29981))
        self.numbers = self.original_numbers.copy()
        random.shuffle(self.numbers)
        self.counter = 0

    def get_unique_number(self):
        # ! This is a sample code to limit the number of requests per specific time period
        # if self.counter >= 3:
        #     time.sleep(60)
        #     self.counter = 0

        if not self.numbers:
            self.reset()

        self.counter += 1
        return self.numbers.pop()

    def reset(self):
        self.numbers = self.original_numbers.copy()
        random.shuffle(self.numbers)


def get(url: str, params=None) -> Response:
    """
    Sends a GET request with Smart Proxy and retries up to 10 times if necessary.

    :param url: URL for the new :class:`Request` object.
    :param params: (optional) Dictionary, list of tuples or bytes to send in the query string for the :class:`Request`.
    :return: :class:`Response <Response>` object
    :rtype: requests.Response
    """

    urn = UniqueRandomNumbers()
    max_attempts = 10
    attempt = 0

    while attempt < max_attempts:
        random_port = urn.get_unique_number()
        proxy_url = f"http://spusfxy185:TwinTwinTwin@eu.dc.smartproxy.com:{random_port}"
        logging.info(f"Attempt {attempt + 1}: Using proxy {proxy_url}")
        proxies = {
            "http": proxy_url,
            "https": proxy_url,
        }

        try:
            response = requests.get(url=url, params=params, proxies=proxies)

            if response.status_code == 200:
                return response
            else:
                logging.error(
                    f"Failed to get {url} with status code {response.status_code}"
                )
                logging.error(f"Response: {response.text}")

        except requests.exceptions.HTTPError as http_err:
            logging.error(
                f"HTTP error occurred، Failed to get {url} with error: {http_err}"
            )
        except Exception as err:
            logging.error(f"Some error occurred during getting {url}, error: {err}")

        attempt += 1

    logging.error(f"All attempts failed for URL: {url}")
    raise Exception(f"All attempts failed for URL: {url}")
