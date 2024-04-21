from typing import Any
from llama_index.core import Document

import unittest

from typing import Any




class SummaryTransformer():
    
    def transform(self, summary: str, metadata: dict[str, Any], **kwargs) -> Document:
       
        pass
class GithubSummaryTransformer(SummaryTransformer):
    def transform(self, summary: str, metadata: dict[str, Any], **kwargs) -> Document:
        excluded_llm_metadata_keys = kwargs.get("excluded_llm_metadata_keys", [])
        excluded_embed_metadata_keys = kwargs.get("excluded_embed_metadata_keys", [])

        document = Document(
            text=summary,
            metadata=metadata,
            excluded_embed_metadata_keys=excluded_embed_metadata_keys,
            excluded_llm_metadata_keys=excluded_llm_metadata_keys,
        )
        return document



class TestGithubSummaryTransformer(unittest.TestCase):

    def test_transform(self):
        # Create an instance of the GithubSummaryTransformer class
        transformer = GithubSummaryTransformer()

        # Define sample data for the test
        summary = "This is a summary"
        metadata = {"key": "value"}
        excluded_llm_metadata_keys = ["key1", "key2"]
        excluded_embed_metadata_keys = ["key3", "key4"]

        # Call the transform method with the sample data
        result = transformer.transform(
            summary,
            metadata,
            excluded_llm_metadata_keys=excluded_llm_metadata_keys,
            excluded_embed_metadata_keys=excluded_embed_metadata_keys
        )

        # Assert that the result is an instance of the Document class
        self.assertIsInstance(result, Document)

        # Add more assertions to validate the output according to your requirements

    def test_transform_with_empty_summary(self):
        transformer = GithubSummaryTransformer()

        summary = ""
        metadata = {"key": "value"}

        result = transformer.transform(summary, metadata)

        # Assert that the result is an instance of the Document class
        self.assertIsInstance(result, Document)

        # Assert that the transformed document has an empty text
        self.assertEqual(result.text, "")

    def test_transform_with_empty_metadata(self):
        transformer = GithubSummaryTransformer()

        summary = "This is a summary"
        metadata = {}

        result = transformer.transform(summary, metadata)

        # Assert that the result is an instance of the Document class
        self.assertIsInstance(result, Document)

        # Assert that the transformed document has the provided summary text
        self.assertEqual(result.text, summary)

        # Assert that the transformed document has an empty metadata
        self.assertEqual(result.metadata, {})

if __name__ == '__main__':
    unittest.main()