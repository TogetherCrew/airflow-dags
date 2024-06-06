import unittest
from wikipedia import set_api_url, page


class TestMediaWikiAPI(unittest.TestCase):

    def test_page_wikipedia(self):
        set_api_url('https://en.wikipedia.org/w/api.php')
        result = page('Python (programming language)')
        print("Wikipedia result:", result)
        print(f"Wikipedia Page Title: {result.title}")
        print(f"Wikipedia Page ID: {result.pageid}")
        print(f"Wikipedia Page URL: {result.url}")
        print(f"Wikipedia Page Body: {result.content[:500]}...")
        self.assertEqual(result.title, 'Python (programming language)')
        self.assertIsNotNone(result.pageid)
        self.assertIsNotNone(result.url)

    def test_page_wikimedia(self):
        set_api_url('https://commons.wikimedia.org/w/api.php')
        result = page('File:Sunset.jpg', auto_suggest=False)
        print("Wikimedia result:", result)
        print(f"Wikimedia Page Title: {result.title}")
        print(f"Wikimedia Page ID: {result.pageid}")
        print(f"Wikimedia Page URL: {result.url}")
        print(f"Wikimedia Page Body: {result.content[:500]}...")
        self.assertIn('Sunset', result.title)  # Check if 'Sunset' is in the title
        self.assertIsNotNone(result.pageid)
        self.assertIsNotNone(result.url)

    def test_page_wikitravel(self):
        set_api_url('http://wikitravel.org/wiki/en/api.php')
        result = page('Paris', auto_suggest=False)
        print("Wikitravel result:", result)
        print(f"Wikitravel Page Title: {result.title}")
        print(f"Wikitravel Page ID: {result.pageid}")
        print(f"Wikitravel Page URL: {result.url}")
        print(f"Wikitravel Page Body: {result.content[:500]}...")  # Print first 500 characters of content
        self.assertEqual(result.title, 'Paris')
        self.assertIsNotNone(result.pageid)
        self.assertIsNotNone(result.url)


if __name__ == '__main__':
    unittest.main()
