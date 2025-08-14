import logging

from llama_index.core import Document, SummaryIndex
from llama_index.core.llms import LLM
from llama_index.core.response_synthesizers.base import BaseSynthesizer


class SummaryBase:
    def __init__(
        self,
        llm: LLM,
        response_synthesizer: BaseSynthesizer | None = None,
        verbose: bool = False,
    ) -> None:
        """
        Summary base class

        Parameters
        -----------
        set_response_synthesizer : bool | None
            whether to set a response_synthesizer to refine the summaries or not
            if nothing passed would be set to None
        verbose : bool
            whether to show the progress of summarizing or not
        llm : LLM
            the llm to use
            if nothing passed, it would use the default `llama_index.core.Setting.llm`

        Note: `chunk_size` is read from `llama_index.core.Setting.chunk_size`.
        """
        self.llm = llm
        self.response_synthesizer = response_synthesizer
        self.verbose = verbose

    def _get_summary(
        self, messages_document: list[Document], summarization_query: str
    ) -> str:
        """
        a simple wrapper to get the summaries of multiple documents
        """
        if not messages_document:
            logging.warning("No documents provided for summarization")
            return ""
        
        if len(messages_document) > 1000:
            logging.warning(f"Large document set ({len(messages_document)} docs) - consider batching")
        
        try:
            summary_index = SummaryIndex.from_documents(
                documents=messages_document,
                response_synthesizer=self.response_synthesizer,
                show_progress=self.verbose,
            )
            summary_response = self.retrieve_summary(summary_index, summarization_query)
            
            # Basic validation of summary quality
            if len(summary_response.strip()) < 10:
                logging.warning("Generated summary appears too short, might be low quality")
            
            return summary_response
            
        except Exception as e:
            logging.error(f"Failed to generate summary: {str(e)}")
            raise e

    def retrieve_summary(
        self,
        doc_summary_index: SummaryIndex,
        query: str,
    ) -> str:
        """
        retrieve a summary of the available documents within the doc_summary_index

        Parameters
        -----------
        doc_summary_index : llama_index.SummaryIndex
            the document summary index with the data nodes available within it
        query : str
            the query to get summary
        """
        try:
            query_engine = doc_summary_index.as_query_engine(
                response_mode="tree_summarize",
                response_synthesizer=self.response_synthesizer,
                llm=self.llm,
            )
            response = query_engine.query(query)
            return response.response
        except Exception as e:
            logging.error(f"Failed to retrieve summary from index: {str(e)}")
            raise e
