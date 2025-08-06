import unittest

from hivemind_etl_helpers.src.utils.sort_summary_docs import sort_summaries_daily
from llama_index.core import Document


class TestSortSummariesDaily(unittest.TestCase):
    def test_empty_inputs(self):
        sorted_docs = sort_summaries_daily([], [], [])
        self.assertEqual(sorted_docs, [])

    def test_summaries_all(self):
        """
        given all level of summaries, sort them
        """
        doc_daily = [
            Document(
                text="Document 1",
                metadata={"date": 1673296200.0, "channel": None, "thread": None},
            ),
            Document(
                text="Document 2",
                metadata={"date": 1672864200.0, "channel": None, "thread": None},
            ),
            Document(
                text="Document 3",
                metadata={"date": 1673728200.0, "channel": None, "thread": None},
            ),
        ]
        doc_channel = [
            Document(
                text="Document 4",
                metadata={"date": 1673296200.0, "channel": "channel#1", "thread": None},
            ),
            Document(
                text="Document 5",
                metadata={"date": 1672864200.0, "channel": "channel#2", "thread": None},
            ),
            Document(
                text="Document 6",
                metadata={"date": 1673728200.0, "channel": "channel#3", "thread": None},
            ),
        ]
        doc_thread = [
            Document(
                text="Document 7",
                metadata={
                    "date": 1673296200.0,
                    "channel": "channel#1",
                    "thread": "thread#1",
                },
            ),
            Document(
                text="Document 8",
                metadata={
                    "date": 1672864200.0,
                    "channel": "channel#2",
                    "thread": "thread#2",
                },
            ),
            Document(
                text="Document 9",
                metadata={
                    "date": 1673728200.0,
                    "channel": "channel#3",
                    "thread": "thread#3",
                },
            ),
        ]
        sorted_docs = sort_summaries_daily(
            level1_docs=doc_thread, level2_docs=doc_channel, daily_docs=doc_daily
        )

        # Expected order after sorting
        expected_order = [
            doc_thread[1],
            doc_channel[1],
            doc_daily[1],
            doc_thread[0],
            doc_channel[0],
            doc_daily[0],
            doc_thread[2],
            doc_channel[2],
            doc_daily[2],
        ]

        # Compare the sorted result with the expected order
        self.assertEqual(sorted_docs, expected_order)

    def test_summaries_ovelapping(self):
        """
        given all level of summaries, sort them
        some docs may have the same date which shouldn't cause any problems
        """
        doc_daily = [
            Document(
                text="Document 1",
                metadata={"date": 1673296200.0, "channel": None, "thread": None},
            ),
            Document(
                text="Document 2",
                metadata={"date": 1672864200.0, "channel": None, "thread": None},
            ),
            Document(
                text="Document 3",
                metadata={"date": 1672864200.0, "channel": None, "thread": None},
            ),
            Document(
                text="Document 4",
                metadata={"date": 1673728200.0, "channel": None, "thread": None},
            ),
        ]
        doc_channel = [
            Document(
                text="Document 5",
                metadata={"date": 1673296200.0, "channel": "channel#1", "thread": None},
            ),
            Document(
                text="Document 6",
                metadata={"date": 1672864200.0, "channel": "channel#2", "thread": None},
            ),
            Document(
                text="Document 7",
                metadata={"date": 1673728200.0, "channel": "channel#3", "thread": None},
            ),
            Document(
                text="Document 8",
                metadata={"date": 1673728200.0, "channel": "channel#3", "thread": None},
            ),
        ]
        doc_thread = [
            Document(
                text="Document 9",
                metadata={
                    "date": 1673296200.0,
                    "channel": "channel#1",
                    "thread": "thread#1",
                },
            ),
            Document(
                text="Document 10",
                metadata={
                    "date": 1672864200.0,
                    "channel": "channel#2",
                    "thread": "thread#2",
                },
            ),
            Document(
                text="Document 11",
                metadata={
                    "date": 1673728200.0,
                    "channel": "channel#3",
                    "thread": "thread#3",
                },
            ),
            Document(
                text="Document 12",
                metadata={
                    "date": 1673728200.0,
                    "channel": "channel#3",
                    "thread": "thread#3",
                },
            ),
        ]
        sorted_docs = sort_summaries_daily(
            level1_docs=doc_thread, level2_docs=doc_channel, daily_docs=doc_daily
        )

        # Expected order after sorting
        expected_order = [
            # 1672864200.0 (2023-01-05)
            doc_thread[1],
            doc_channel[1],
            doc_daily[1],
            doc_daily[2],
            # 1673296200.0 (2023-01-10)
            doc_thread[0],
            doc_channel[0],
            doc_daily[0],
            # 1673728200.0 (2023-01-15)
            doc_thread[2],
            doc_thread[3],
            doc_channel[2],
            doc_channel[3],
            doc_daily[3],
        ]

        # Compare the sorted result with the expected order
        self.assertEqual(sorted_docs, expected_order)

    def test_summaries_daily(self):
        # Create sample documents with different dates
        doc1 = Document(
            text="Document 1",
            metadata={"date": 1673296200.0, "channel": None, "thread": None},
        )
        doc2 = Document(
            text="Document 2",
            metadata={"date": 1672864200.0, "channel": None, "thread": None},
        )
        doc3 = Document(
            text="Document 3",
            metadata={"date": 1673728200.0, "channel": None, "thread": None},
        )

        # Call the function with unsorted documents
        unsorted_docs = [doc1, doc2, doc3]
        sorted_docs = sort_summaries_daily([], [], unsorted_docs)

        # Expected order after sorting
        expected_order = [doc2, doc1, doc3]

        # Compare the sorted result with the expected order
        self.assertEqual(sorted_docs, expected_order)

    def test_summaries_sorted_length(self):
        doc_daily = [
            Document(
                text="Document 1",
                metadata={"date": 1673296200.0, "channel": None, "thread": None},
            ),
            Document(
                text="Document 2",
                metadata={"date": 1672864200.0, "channel": None, "thread": None},
            ),
            Document(
                text="Document 3",
                metadata={"date": 1672864200.0, "channel": None, "thread": None},
            ),
            Document(
                text="Document 4",
                metadata={"date": 1673728200.0, "channel": None, "thread": None},
            ),
        ]
        doc_channel = [
            Document(
                text="Document 5",
                metadata={"date": 1673296200.0, "channel": "channel#1", "thread": None},
            ),
            Document(
                text="Document 6",
                metadata={"date": 1672864200.0, "channel": "channel#2", "thread": None},
            ),
            Document(
                text="Document 7",
                metadata={"date": 1673728200.0, "channel": "channel#3", "thread": None},
            ),
            Document(
                text="Document 8",
                metadata={"date": 1673728200.0, "channel": "channel#3", "thread": None},
            ),
        ]
        doc_thread = [
            Document(
                text="Document 9",
                metadata={
                    "date": 1673296200.0,
                    "channel": "channel#1",
                    "thread": "thread#1",
                },
            ),
            Document(
                text="Document 10",
                metadata={
                    "date": 1672864200.0,
                    "channel": "channel#2",
                    "thread": "thread#2",
                },
            ),
            Document(
                text="Document 11",
                metadata={
                    "date": 1673728200.0,
                    "channel": "channel#3",
                    "thread": "thread#3",
                },
            ),
            Document(
                text="Document 12",
                metadata={
                    "date": 1673728200.0,
                    "channel": "channel#3",
                    "thread": "thread#3",
                },
            ),
        ]
        sorted_docs = sort_summaries_daily(
            level1_docs=doc_thread, level2_docs=doc_channel, daily_docs=doc_daily
        )

        self.assertEqual(
            len(sorted_docs), len(doc_daily) + len(doc_channel) + len(doc_thread)
        )
