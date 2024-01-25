import unittest

from hivemind_etl_helpers.src.db.discord.utils.content_parser import (
    check_no_content_only_links,
)


class TestCheckNoContentOnlyLinks(unittest.TestCase):
    def test_check_contents_default(self):
        content = "This is a [URL1] sample [URL2] string with [URL3] links."
        no_content = check_no_content_only_links(content)
        self.assertEqual(no_content, False)

    def test_check_conetents_pattern_custom(self):
        content = ": @#)*)@# [CUSTOM123]"
        pattern = r"\[CUSTOM\d+\]"
        no_content = check_no_content_only_links(content, pattern)
        self.assertEqual(no_content, True)

    def test_check_contents_no_pattern_match(self):
        content = "No pattern to remove here."
        no_content = check_no_content_only_links(content)
        self.assertEqual(no_content, False)

    def test_check_contents_empty_string(self):
        content = ""
        no_content = check_no_content_only_links(content)
        self.assertEqual(no_content, True)

    def test_check_contents_empty_pattern(self):
        content = "This is a [URL1] sample [URL2] string with [URL3] links."
        pattern = ""
        no_content = check_no_content_only_links(content, pattern)
        self.assertEqual(no_content, False)

    def test_check_contents_unseparated(self):
        content = "This is a [URL1]sample [URL2]string with [URL3] links."
        no_content = check_no_content_only_links(content)
        self.assertEqual(no_content, False)

    def test_check_contents_just_urls(self):
        content = "[URL1] [URL2][URL3]"
        no_content = check_no_content_only_links(content)
        self.assertEqual(no_content, True)

    def test_check_contents_unseparated_just_urls(self):
        content = "<[URL1]> [URL2][URL3]"
        no_content = check_no_content_only_links(content)
        self.assertEqual(no_content, True)
