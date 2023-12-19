from llama_index import Document, ServiceContext, SummaryIndex
from llama_index.llms import LLM, OpenAI
from llama_index.response_synthesizers.base import BaseSynthesizer


class SummaryBase:
    def __init__(
        self,
        service_context: ServiceContext | None = None,
        response_synthesizer: BaseSynthesizer | None = None,
        llm: LLM | None = None,
        verbose: bool = False,
    ) -> None:
        """
        Summary base class

        Parameters
        -----------
        service_context : llama_index.ServiceContext | None
            the service context for llama_index to work
            if nothing passed will be to `llm=gpt-3.5-turbo` and `chunk_size = 512`
        set_response_synthesizer : bool | None
            whether to set a response_synthesizer to refine the summaries or not
            if nothing passed would be set to None
        llm : LLM | None
            the llm to use
            if nothing passed, it would use chatgpt with `gpt-3.5-turbo` model
        verbose : bool
            whether to show the progress of summarizing or not
        testing : bool
            testing mode would use a MockLLM
        """
        if llm is None:
            llm = OpenAI(temperature=0, model="gpt-3.5-turbo")

        if service_context is None:
            service_context = ServiceContext.from_defaults(llm=llm, chunk_size=512)

        self.service_context = service_context
        self.response_synthesizer = response_synthesizer
        self.llm = llm
        self.verbose = verbose

    def _get_summary(
        self, messages_document: list[Document], summarization_query: str
    ) -> str:
        """
        a simple wrapper to get the summaries of multiple documents
        """
        summary_index = SummaryIndex.from_documents(
            documents=messages_document,
            response_synthesizer=self.response_synthesizer,
            service_context=self.service_context,
            show_progress=self.verbose,
        )
        summary_response = self.retrieve_summary(summary_index, summarization_query)
        return summary_response

    def retrieve_summary(
        self,
        doc_summary_index: SummaryIndex,
        query: str,
    ) -> str:
        """
        retreive a summary of the available documents within the doc_summary_index

        Parameters
        -----------
        doc_summary_index : llama_index.SummaryIndex
            the document summary index with the data nodes available within it
        query : str
            the query to get summary
        """
        query_engine = doc_summary_index.as_query_engine(
            response_mode="tree_summarize",
            response_synthesizer=self.response_synthesizer,
        )
        response = query_engine.query(query)
        return response.response
