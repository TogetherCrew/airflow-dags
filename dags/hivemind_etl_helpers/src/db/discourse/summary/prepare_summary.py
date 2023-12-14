import logging

from llama_index import Document, ServiceContext
from llama_index.llms import LLM
from llama_index.response_synthesizers.base import BaseSynthesizer
from hivemind_etl_helpers.src.utils.summary_base import SummaryBase
from hivemind_etl_helpers.src.db.discourse.raw_post_to_documents import (
    transform_raw_to_documents,
)
from hivemind_etl_helpers.src.db.discourse.summary.summary_utils import (
    transform_summary_to_document,
)
import neo4j


class DiscourseSummary(SummaryBase):
    def __init__(
        self,
        forum_id: str,
        service_context: ServiceContext | None = None,
        response_synthesizer: BaseSynthesizer | None = None,
        llm: LLM | None = None,
        verbose: bool = False,
    ) -> None:
        super().__init__(service_context, response_synthesizer, llm, verbose)
        self.prefix = f"FORUM_ID: {forum_id} "

    def prepare_topic_summaries(
        self,
        raw_data_grouped: list[neo4j._data.Record],
        summarization_query: str,
    ) -> dict[str, dict[str, dict[str, str]]]:
        """
        prepare the topic sumaries

        Parameters
        ------------
        raw_data : list[neo4j._data.Record]
            the fetched raw data from discourse
        """
        logging.info(f"{self.prefix}Preparing the topic summaries!")

        topic_summaries: dict[str, dict[str, dict[str, str]]] = {}

        for record in raw_data_grouped:
            date = record["date"]
            posts = record["posts"]

            # data preparation
            # first level key: category
            # second level key: topic
            # third level values are the actual posts
            category_topic_grouped: dict[str, dict[str, list[dict[str, str]]]] = {}
            for post in posts:
                category_topic_grouped.setdefault(post["category"], {}).setdefault(
                    post["topic"], []
                )
                category_topic_grouped[post["category"]][post["topic"]].append(post)

            topic_summaries.setdefault(date, {})

            for category in category_topic_grouped.keys():
                topic_summaries[date].setdefault(category, {})

                for topic in category_topic_grouped[category]:
                    topic_posts = category_topic_grouped[category][topic]
                    topic_post_documents = transform_raw_to_documents(topic_posts)
                    summary = self._get_summary(
                        topic_post_documents, summarization_query
                    )

                    topic_summaries[date][category][topic] = summary

        return topic_summaries

    def prepare_category_summaries(
        self,
        topic_summaries: dict[str, dict[str, dict[str, str]]],
        summarization_query: str,
    ) -> tuple[dict[str, dict[str, str]], list[Document]]:
        """
        prepare summaries per category

        Parameters
        -----------
        topic_summaries : dict[str, dict[str, dict[str, str]]]
            the topic summaries
            the variable is per day, category, and topic
        summarization_query : str
            the summarization query to do on the LLM

        Returns
        --------
        category_summaries : dict[str, dict[str, str]]
            the category summaries per date
        topic_summary_documents : list[llama_index.Document]
            a list of documents for topic summaries
        """
        logging.info(f"{self.prefix}Preparing the topic summaries")

        topic_summary_documents: list[Document] = []
        category_summaries: dict[str, dict[str, str]] = {}

        for date in topic_summaries:
            category_summaries.setdefault(date, {})

            for category in topic_summaries[date].keys():
                category_topic_summary_documents: list[Document] = []

                for topic in topic_summaries[date][category].keys():
                    topic_document = transform_summary_to_document(
                        summary=topic_summaries[date][category][topic],
                        date=date,
                        topic=topic,
                        category=category,
                    )
                    category_topic_summary_documents.append(topic_document)

                topic_summary_documents.extend(category_topic_summary_documents)

                # if there was just one topic
                # the summary of the topic would be the summary of the category
                summary: str
                if len(category_topic_summary_documents) == 1:
                    summary = category_topic_summary_documents[0].text
                else:
                    summary = self._get_summary(
                        category_topic_summary_documents, summarization_query
                    )

                category_summaries[date][category] = summary

        return category_summaries, topic_summary_documents

    def prepare_daily_summaries(
        self,
        category_summaries: dict[str, dict[str, str]],
        summarization_query: str,
    ) -> tuple[dict[str, str], list[Document]]:
        """
        prepare daily summaries

        Parameters
        -----------
        category_summaries : dict[str, dict[str, str]]
            the summaries per day, and category
        summarization_query : str
            the summarization query to do on the LLM

        Returns
        ---------
        daily_summaries : dict[str, str]
            the summaries per day for different category
        category_summary_documenets : list[llama_index.Document]
            a list of documents related to the summaries of the category
        """
        logging.info(f"{self.prefix}Preparing the daily summaries")

        daily_summaries: dict[str, str] = {}
        category_summary_documenets: list[Document] = []

        for date in category_summaries.keys():
            day_category_documents: list[Document] = []

            for category in category_summaries[date].keys():
                cat_summary = category_summaries[date][category]
                cat_document = transform_summary_to_document(
                    summary=cat_summary,
                    date=date,
                    category=category,
                )
                day_category_documents.append(cat_document)

            category_summary_documenets.extend(day_category_documents)

            summary: str
            if len(day_category_documents) == 1:
                summary = day_category_documents[0].text
            else:
                summary = self._get_summary(day_category_documents, summarization_query)

            daily_summaries[date] = summary

        return daily_summaries, category_summary_documenets

    def prepare_daily_summary_documents(
        self, daily_summaries: dict[str, str]
    ) -> list[Document]:
        """
        prepare the documents for daily summaries of discourse

        Parameters
        -----------
        daily_summaries : dict[str, str]
            the summaries per day for different category


        Returns
        ---------
        daily_summary_documents : list[llama_index.Document]
            a list of documents related to the daily summaries of discourse
        """
        daily_summary_documents: list[Document] = []
        for date in daily_summaries.keys():
            day_document = transform_summary_to_document(
                summary=daily_summaries[date],
                date=date,
            )
            daily_summary_documents.append(day_document)

        return daily_summary_documents
