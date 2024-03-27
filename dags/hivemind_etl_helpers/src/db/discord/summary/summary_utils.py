from typing import Any
from llama_index.core import Document
from hivemind_etl_helpers.src.utils.summary.summary_transformer import (
    SummaryTransformer,
)


class DiscordSummaryTransformer(SummaryTransformer):

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

    def transform_thread_summary_to_document(
        self,
        thread_name: str,
        thread_summary: str,
        summary_date: str,
        thread_channel: str,
    ) -> Document:
        """
        prepare the thread summary documents

        Parameters
        -----------
        thread_name : str
            the related dicord thread name
        thread_summary : str
            the related summary for the thread
        summary_date : str
            the date for the summary
        thread_channel : str
            the channel related to the thread

        Returns
        ---------
        thread_summary_document : llama_index.Document
            the llama_index document created for thread summary
        """
        thread_summary_document = self.transform(
            summary=thread_summary,
            metadata={
                "date": summary_date,
                "thread": thread_name,
                "channel": thread_channel,
                "type": "thread",
            },
            excluded_embed_metadata_keys=["date", "thread", "channel", "type"],
        )

        return thread_summary_document

    def transform_channel_summary_to_document(
        self,
        channel_name: str,
        channel_summary: str,
        summary_date: str,
    ) -> Document:
        """
        prepare the channel summary document

        Parameters
        -----------
        channel_name : str
            the related dicord thread name
        channel_summary : str
            the related summary for the thread
        summary_date : str
            the date for the summary

        Returns
        ---------
        channel_summary_document : llama_index.Document
            the llama_index document created for thread summary
        """

        channel_summary_document = self.transform(
            summary=channel_summary,
            metadata={"date": summary_date, "channel": channel_name, "type": "channel"},
            excluded_embed_metadata_keys=["date", "thread", "channel", "type"],
        )

        return channel_summary_document

    def transform_daily_summary_to_document(
        self,
        daily_summary: dict[str, str],
    ) -> list[Document]:
        """
        prepare the daily summary document

        Parameters
        -----------
        daily_summary : dict[str, str]
            the summary of each date
            they keys are the date in format `%Y-%m-%d`

        Returns
        ---------
        daily_summary_documents : list[llama_index.Document]
            the llama_index document created for thread summary
        """

        daily_summary_documents: list[Document] = []

        for date in daily_summary.keys():
            summary = daily_summary[date]
            doc = self.transform(
                summary=summary,
                metadata={"date": date, "type": "day"},
                excluded_embed_metadata_keys=["date", "thread", "channel", "type"],
            )
            daily_summary_documents.append(doc)

        return daily_summary_documents
