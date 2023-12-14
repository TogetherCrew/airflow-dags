import unittest

from hivemind_etl_helpers.src.db.discord.utils.prepare_raw_message_urls import (
    extract_urls,
)


class TestExtractUrls(unittest.TestCase):
    def test_extract_urls(self):
        text = "Check out this website: https://www.example.com. Another link: http://www.google.com"
        urls = extract_urls(text)

        expected_urls = ["https://www.example.com.", "http://www.google.com"]
        self.assertEqual(urls, expected_urls)

    def test_extract_urls_empty_text(self):
        text = ""
        urls = extract_urls(text)

        self.assertEqual(urls, [])

    def test_extract_urls_no_scheme(self):
        text = "Visit www.example.com for more information."
        urls = extract_urls(text)

        self.assertEqual(urls, [])

    def test_extract_urls_no_netloc(self):
        text = "Check out this website: https:///path/to/page.html"
        urls = extract_urls(text)

        self.assertEqual(urls, [])
