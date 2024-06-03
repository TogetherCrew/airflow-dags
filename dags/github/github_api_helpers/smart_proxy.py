import logging
import random

import requests
from github.credentials import load_smart_proxy_url


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


def get(url: str, params=None) -> requests.Response:
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
        url = load_smart_proxy_url()
        proxy_url = f"{url}:{random_port}"
        logging.info(f"Attempt {attempt + 1}/{max_attempts}: Using smart proxy!")
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
                    f"Failed to do get using smart proxy with status code {response.status_code}"
                )
                logging.error(f"Response: {response.text}")

        except requests.exceptions.HTTPError as http_err:
            logging.error(
                f"HTTP error occurred، Failed to get using smart proxy. Error: {http_err}"
            )
        except Exception as err:
            logging.error(f"Exception occurred while using smart proxy. Error: {err}")

        attempt += 1
    raise Exception("All attempts failed using smart proxy!")
