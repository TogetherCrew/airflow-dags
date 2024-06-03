from unittest import TestCase

from github.credentials import load_smart_proxy_url


class TestCredentialsLoading(TestCase):
    def test_smart_proxy_url_type(self):
        url = load_smart_proxy_url()
        self.assertIsInstance(url, str)

    def test_smart_proxy_url_no_none(self):
        url = load_smart_proxy_url()
        self.assertNotIn("None", url)
