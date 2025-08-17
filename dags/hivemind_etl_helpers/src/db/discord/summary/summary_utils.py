from datetime import datetime
from typing import Any
import uuid

from hivemind_etl_helpers.src.utils.summary.summary_transformer import (
    SummaryTransformer,
)
from llama_index.core import Document


class DiscordSummaryTransformer(SummaryTransformer):
    def transform(self, summary: str, metadata: dict[str, Any], **kwargs) -> Document:
        excluded_llm_metadata_keys = kwargs.get("excluded_llm_metadata_keys", [])
        excluded_embed_metadata_keys = kwargs.get("excluded_embed_metadata_keys", [])
        doc_id = kwargs.get("doc_id", str(uuid.uuid4()))

        document = Document(
            text=summary,
            metadata=metadata,
            excluded_embed_metadata_keys=excluded_embed_metadata_keys,
            excluded_llm_metadata_keys=excluded_llm_metadata_keys,
            doc_id=doc_id,
        )

        return document

    def transform_thread_summary_to_document(
        self,
        thread_name: str,
        thread_summary: str,
        summary_date: float | str,
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
        summary_date : float | str
            the date for the summary (timestamp or YYYY-MM-DD string)
        thread_channel : str
            the channel related to the thread

        Returns
        ---------
        thread_summary_document : llama_index.Document
            the llama_index document created for thread summary
            The document will have a date metadata in YYYY-MM-DD format
        """
        # Convert to YYYY-MM-DD format
        if isinstance(summary_date, str):
            formatted_date = summary_date
        else:
            formatted_date = datetime.fromtimestamp(summary_date).strftime("%Y-%m-%d")
        
        thread_summary_document = self.transform(
            doc_id = f"{formatted_date}-{thread_name}-{thread_channel}",
            summary=thread_summary,
            metadata={
                "date": formatted_date,
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
        summary_date: float | str,
    ) -> Document:
        """
        prepare the channel summary document

        Parameters
        -----------
        channel_name : str
            the related dicord thread name
        channel_summary : str
            the related summary for the thread
        summary_date : float | str
            the date for the summary (timestamp or YYYY-MM-DD string)

        Returns
        ---------
        channel_summary_document : llama_index.Document
            the llama_index document created for thread summary
            The document will have a date metadata in YYYY-MM-DD format
        """
        # Convert to YYYY-MM-DD format
        if isinstance(summary_date, str):
            formatted_date = summary_date
        else:
            formatted_date = datetime.fromtimestamp(summary_date).strftime("%Y-%m-%d")

        channel_summary_document = self.transform(
            doc_id = f"{formatted_date}-{channel_name}",
            summary=channel_summary,
            metadata={"date": formatted_date, "channel": channel_name, "type": "channel"},
            excluded_embed_metadata_keys=["date", "thread", "channel", "type"],
        )

        return channel_summary_document

    def transform_daily_summary_to_document(
        self,
        daily_summary: dict[float | str, str],
    ) -> list[Document]:
        """
        prepare the daily summary document

        Parameters
        -----------
        daily_summary : dict[float | str, str]
            the summary of each date
            they keys are the date in format `float` (timestamp) or `str` (YYYY-MM-DD)

        Returns
        ---------
        daily_summary_documents : list[llama_index.Document]
            the llama_index document created for thread summary
            Each document will have a date metadata in YYYY-MM-DD format
        """

        daily_summary_documents: list[Document] = []

        for date in daily_summary.keys():
            summary = daily_summary[date]
            # Convert to YYYY-MM-DD format
            if isinstance(date, str):
                formatted_date = date
            else:
                formatted_date = datetime.fromtimestamp(date).strftime("%Y-%m-%d")
            
            doc = self.transform(
                doc_id = f"{formatted_date}-day",
                summary=summary,
                metadata={"date": formatted_date, "type": "day"},
                excluded_embed_metadata_keys=["date", "thread", "channel", "type"],
            )
            daily_summary_documents.append(doc)

        return daily_summary_documents
