from typing import List
from custom_wikipedia_reader import WikipediaReader


# Define a function to test WikipediaReader with Wikivoyage
def test_wikipedia_reader_with_wikivoyage(api_url: str, pages: List[str]):
    # Initialize the WikipediaReader with the given API URL
    reader = WikipediaReader(api_url=api_url)

    # Load data for the given pages
    documents = reader.load_data(pages)

    # Print out the results
    for doc in documents:
        print(f"Document ID: {doc.id_}")
        print(f"Document Text: {doc.text[:500]}...")  # Print first 500 characters for brevity

# Example page title from Wikivoyage to test
pages_to_test = ["Serbia"]

# Call the test function with the Wikivoyage API URL and the example page
test_wikipedia_reader_with_wikivoyage("https://en.wikivoyage.org/w/api.php", pages_to_test)