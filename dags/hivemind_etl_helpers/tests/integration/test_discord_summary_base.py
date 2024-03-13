import unittest
from unittest.mock import Mock

from hivemind_etl_helpers.src.utils.summary_base import SummaryBase
from llama_index.core import Document, MockEmbedding, Settings, SummaryIndex
from llama_index.core.llms import MockLLM


class TestSummaryBase(unittest.TestCase):
    def setUp(self):
        # Set up a sample Settings for testing
        Settings.llm = MockLLM()
        Settings.chunk_size = 512
        Settings.embed_model = MockEmbedding(embed_dim=1024)

    def test_summary_base_default_values(self):
        # Test the default values of the SummaryBase class
        # We need to set the service_context as we need the MockEmbedding model
        self.setUp()
        summary_base = SummaryBase(llm=MockLLM())
        self.assertIsNone(summary_base.response_synthesizer)
        self.assertIsNotNone(summary_base.llm)
        self.assertFalse(summary_base.verbose)

    def test_summary_base_custom_values(self):
        # Test the SummaryBase class with custom values
        llm_mock = MockLLM()
        response_synthesizer_mock = Mock()
        summary_base = SummaryBase(
            response_synthesizer=response_synthesizer_mock,
            llm=llm_mock,
            verbose=True,
        )
        self.assertEqual(summary_base.response_synthesizer, response_synthesizer_mock)
        self.assertEqual(summary_base.llm, llm_mock)
        self.assertTrue(summary_base.verbose)

    def test_get_summary(self):
        # Test the _get_summary method
        summary_base = SummaryBase(llm=Settings.llm)
        messages_document = [Document(text="Document 1"), Document(text="Document 2")]

        result = summary_base._get_summary(
            messages_document=messages_document,
            summarization_query="Please give me a summary!",
        )
        self.assertIsInstance(result, str)

    def test_retrieve_summary(self):
        # Test the retrieve_summary method
        summary_base = SummaryBase(llm=MockLLM())
        doc_summary_index = SummaryIndex.from_documents(
            documents=[Document(text="Document 1"), Document(text="Document 2")],
        )
        query = "Summarize this"
        result = summary_base.retrieve_summary(doc_summary_index, query)
        self.assertIsInstance(result, str)
