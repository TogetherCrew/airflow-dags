from datetime import datetime

from hivemind_etl_helpers.src.db.discord.summary.prepare_grouped_data import (
    prepare_grouped_data,
)
from hivemind_etl_helpers.src.db.discord.summary.prepare_summaries import (
    PrepareSummaries,
)
from hivemind_etl_helpers.src.db.discord.summary.summary_utils import (
    transform_daily_summary_to_document,
)
from llama_index import Document, ServiceContext
from llama_index.llms import LLM
from llama_index.response_synthesizers.base import BaseSynthesizer


class DiscordSummary(PrepareSummaries):
    def __init__(
        self,
        service_context: ServiceContext | None = None,
        response_synthesizer: BaseSynthesizer | None = None,
        llm: LLM | None = None,
        verbose: bool = False,
    ) -> None:
        """
        initialize the summary preparation class

        Parameters
        -----------
        service_context : llama_index.ServiceContext | None
            the service context for llama_index to work
            if nothing passed will be to `llm=gpt-3.5-turbo` and `chunk_size = 512`
        set_response_synthesizer : BaseSynthesizer | None
            whether to set a response_synthesizer to refine the summaries or not
            if nothing passed would be set to `None`
        llm : LLM | None
            the llm to use
            if nothing passed, it would use chatgpt with `gpt-3.5-turbo` model
        verbose : bool
            whether to show the progress of summarizing or not
        """
        super().__init__(service_context, response_synthesizer, llm, verbose)

    def prepare_summaries(
        self,
        guild_id: str,
        summarization_prefix: str,
        from_date: datetime | None = None,
    ) -> tuple[list[Document], list[Document], list[Document],]:
        """
        prepare per thread summaries of discord messages.
        Note: This will always process the data until 1 day ago.

        Parameters
        ------------
        guild_id : str
            the guild id to access data
        summarization_prefix : str
            the summarization query prefix to do on the LLM
        from_date : datetime
            get the raw data from a specific date
            default is None, meaning get all the messages


        Returns
        ---------
        thread_summaries_documents : list[llama_index.Document]
            list of thread summaries converted to llama_index documents
        channel_summary_documenets : list[llama_index.Document]
            list of channel summaries converted to llama_index documents
        daily_summary_documenets : list[llama_index.Document]
            list of daily summaries converted to llama_index documents
        """
        raw_data_grouped = prepare_grouped_data(guild_id, from_date)
        thread_summaries = self.prepare_thread_summaries(
            guild_id, raw_data_grouped, summarization_prefix + " discord thread"
        )
        (channel_summaries, thread_summary_documenets) = self.prepare_channel_summaries(
            thread_summaries,
            summarization_prefix + " selection of discord thread summaries",
        )
        (daily_summaries, channel_summary_documenets) = self.prepare_daily_summaries(
            channel_summaries,
            summarization_prefix + " selection of discord channel summaries",
        )
        daily_summary_documents = transform_daily_summary_to_document(daily_summaries)

        return (
            thread_summary_documenets,
            channel_summary_documenets,
            daily_summary_documents,
        )
