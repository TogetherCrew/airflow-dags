import unittest

from hivemind_etl_helpers.src.db.discord.utils.prepare_raw_message_urls import (
    prepare_raw_message_urls,
)


class TestRawMessagUrlExtraction(unittest.TestCase):
    def test_normal_message_single_url_http(self):
        msg = "Here you can have a look http://google.com"

        msg_updated, url_reference = prepare_raw_message_urls(msg)

        self.assertEqual(msg_updated, "Here you can have a look [URL0]")
        self.assertEqual(url_reference, {"[URL0]": "http://google.com"})

    def test_normal_messag_single_url_https(self):
        msg = "Here you can have a look https://google.com"

        msg_updated, url_reference = prepare_raw_message_urls(msg)

        self.assertEqual(msg_updated, "Here you can have a look [URL0]")
        self.assertEqual(url_reference, {"[URL0]": "https://google.com"})

    def test_normal_message_multiple_url(self):
        msg = "Here you can have a look https://google.com https://example.com"

        msg_updated, url_reference = prepare_raw_message_urls(msg)

        self.assertEqual(msg_updated, "Here you can have a look [URL0] [URL1]")
        self.assertEqual(
            url_reference,
            {"[URL0]": "https://google.com", "[URL1]": "https://example.com"},
        )

    def test_message_multiple_url_wrappend(self):
        msg = "Here you can have a look <https://google.com> <https://example.com>"

        msg_updated, url_reference = prepare_raw_message_urls(msg)

        self.assertEqual(msg_updated, "Here you can have a look <[URL0]> <[URL1]>")
        self.assertEqual(
            url_reference,
            {"[URL0]": "https://google.com", "[URL1]": "https://example.com"},
        )
