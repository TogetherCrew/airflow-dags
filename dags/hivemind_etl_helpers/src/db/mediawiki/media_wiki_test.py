import unittest
from hivemind_etl_helpers.src.db.mediawiki.extractor import MediaWikiExtractor


class TestMediaWikiExtractor(unittest.TestCase):

    def test_wikipedia_extraction(self):
        # Initialize the extractor with Wikipedia API
        extractor = MediaWikiExtractor(api_url="https://en.wikipedia.org/w/api.php")
        page_ids = ["Banana", "Canada"]
        result = extractor.extract(page_ids=page_ids)

        # Print the results
        for doc in result:
            print(f"Wikipedia Document: {doc.text[:500]}")  # Print the first 500 characters

        # Verify the results
        # self.assertTrue(len(result) > 0)
        # self.assertIn("Python", result[0].text)

    def test_wikimedia_extraction(self):
        # Initialize the extractor with Wikimedia API
        extractor = MediaWikiExtractor(api_url='https://commons.wikimedia.org/w/api.php')
        page_ids = ["File:Sunset.jpg"]
        result = extractor.extract(page_ids=page_ids)

        # Print the results
        for doc in result:
            print(f"Wikimedia Document: {doc.text[:500]}")  # Print the first 500 characters

        # Verify the results
        # self.assertTrue(len(result) > 0)
        # self.assertIn("Example", result[0].text)

    def test_wikidata_extraction(self):
        extractor = MediaWikiExtractor(api_url="http://wikitravel.org/wiki/en/api.php")
        page_ids = ["Paris"]
        result = extractor.extract(page_ids=page_ids)
        for doc in result:
            print(f"Raw WikiTravel Document: {doc}")
            if hasattr(doc, 'text'):
                print(f"WikiTravel Document Text: {doc.text[:500]}")
            else:
                print("Document does not have 'text' attribute")

        self.assertTrue(len(result) > 0, "No documents were returned for WikiTravel extraction.")
        if result and hasattr(result[0], 'text'):
            self.assertTrue(len(result[0].text) > 0, "No content found in WikiTravel document.")

    def test_empty_page_ids(self):
        # Initialize the extractor with the default API
        extractor = MediaWikiExtractor()
        result = extractor.extract(page_ids=[])

        # Print the results
        print(f"Empty Page IDs Result: {result}")

        # Verify the results
        # self.assertEqual(len(result), 0)

if __name__ == '__main__':
    unittest.main()
