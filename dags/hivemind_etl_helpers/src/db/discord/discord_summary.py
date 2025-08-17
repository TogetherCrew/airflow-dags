import logging
from datetime import datetime
from typing import Iterator

from hivemind_etl_helpers.src.db.discord.summary.prepare_grouped_data import (
    prepare_grouped_data,
)
from hivemind_etl_helpers.src.db.discord.summary.prepare_summaries import (
    PrepareSummaries,
)
from hivemind_etl_helpers.src.db.discord.summary.summary_utils import (
    DiscordSummaryTransformer,
)
from llama_index.core import Document, Settings
from llama_index.core.response_synthesizers.base import BaseSynthesizer


class DiscordSummary(PrepareSummaries):
    # Shared prompt components
    PROMPT_HEADER = "You are a social media summarizer."
    
    COMMON_GUIDELINES = (
        "- Capture concrete facts (who/what/when/where), decisions, outcomes, blockers, and action items.\n"
        "- Note counts if many users mention the same thing (e.g., \"~12 mentions\").\n"
        "- Resolve pronouns to canonical entities when obvious.\n"
        "- Include important dates/times and hashtags only if they add meaning.\n"
        "- No speculation; avoid quotes unless essential."
    )
    
    OUTPUT_FORMAT = (
        "Output:\n"
        "- 5-10 concise bullets (one line each), most important first.\n"
        "- Each bullet starts with a bold topic tag in brackets, then the point.\n"
        "- No preamble or conclusion."
    )
    
    # Thread-specific guidelines
    THREAD_GUIDELINES = (
        "- Merge overlapping/duplicate posts; remove noise, greetings, emojis, and links.\n"
        "- Group by topic; surface consensus and notable disagreements."
    )
    
    # Channel-specific guidelines  
    CHANNEL_GUIDELINES = (
        "- Merge related thread topics; identify overarching themes and patterns.\n"
        "- Surface key discussions, decisions, and outcomes across all threads.\n"
        "- Note recurring topics and user participation patterns.\n"
        "- Resolve context from thread summaries to create coherent narrative.\n"
        "- Include important announcements, milestones, and blockers.\n"
        "- No speculation; focus on factual content from thread summaries."
    )
    
    # Daily-specific guidelines
    DAILY_GUIDELINES = (
        "- Merge related channel activities; identify cross-channel themes and coordination.\n"
        "- Surface key daily highlights, major decisions, and community-wide outcomes.\n"
        "- Capture important announcements, project milestones, and collaboration efforts.\n"
        "- Note significant user activities and community engagement patterns.\n"
        "- Resolve context from channel summaries to create unified daily narrative.\n"
        "- Include critical updates, blockers, and action items that impact the community.\n"
        "- No speculation; focus on factual content from channel summaries."
    )

    def __init__(
        self,
        response_synthesizer: BaseSynthesizer | None = None,
        verbose: bool = False,
        **kwargs,
    ) -> None:
        """
        initialize the summary preparation class

        Parameters
        -----------
        set_response_synthesizer : BaseSynthesizer | None
            whether to set a response_synthesizer to refine the summaries or not
            if nothing passed would be set to `None`
        verbose : bool
            whether to show the progress of summarizing or not
        **kwargs :
            llm : LLM
                the llm to use
                if nothing passed, it would use the default `llama_index.core.Setting.llm`

        Note: `chunk_size` is read from `llama_index.core.Setting.chunk_size`.
        """
        llm = kwargs.get("llm", Settings.llm)
        self.discord_summary_transformer = DiscordSummaryTransformer()

        super().__init__(
            llm=llm, response_synthesizer=response_synthesizer, verbose=verbose
        )
    
    def _build_thread_prompt(self) -> str:
        """Build the thread summarization prompt using shared components."""
        return (
            f"{self.PROMPT_HEADER} Given the raw Discord thread messages below, "
            f"produce a clear, de-duplicated bullet-point summary optimized for retrieval.\n\n"
            f"Guidelines:\n"
            f"{self.THREAD_GUIDELINES}\n"
            f"{self.COMMON_GUIDELINES}\n\n"
            f"{self.OUTPUT_FORMAT}\n\n"
            f"Summarize these Discord thread messages:"
        )
    
    def _build_channel_prompt(self) -> str:
        """Build the channel summarization prompt using shared components."""
        return (
            f"{self.PROMPT_HEADER} Given the Discord thread summaries below from a single channel, "
            f"produce a comprehensive channel-level summary optimized for retrieval.\n\n"
            f"Guidelines:\n"
            f"{self.CHANNEL_GUIDELINES}\n"
            f"{self.COMMON_GUIDELINES}\n\n"
            f"{self.OUTPUT_FORMAT}\n\n"
            f"Summarize these Discord channel thread summaries:"
        )
    
    def _build_daily_prompt(self) -> str:
        """Build the daily summarization prompt using shared components."""
        return (
            f"{self.PROMPT_HEADER} Given the Discord channel summaries below from a single day, "
            f"produce a comprehensive daily summary optimized for retrieval.\n\n"
            f"Guidelines:\n"
            f"{self.DAILY_GUIDELINES}\n"
            f"{self.COMMON_GUIDELINES}\n\n"
            f"{self.OUTPUT_FORMAT}\n\n"
            f"Summarize these Discord daily channel summaries:"
        )

    def prepare_summaries(
        self,
        guild_id: str,
        selected_channels: list[str],
        from_date: datetime,
    ) -> tuple[list[Document], list[Document], list[Document],]:
        """
        prepare per thread summaries of discord messages.
        Note: This will always process the data until 1 day ago.

        Parameters
        ------------
        guild_id : str
            the guild id to access data
        selected_channels: list[str]
            the discord channels to produce summaries
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
        raw_data_grouped = prepare_grouped_data(guild_id, from_date, selected_channels)
        if raw_data_grouped != {}:
            thread_summaries = self.prepare_thread_summaries(
                guild_id,
                raw_data_grouped,
                self._build_thread_prompt(),
            )
            (
                channel_summaries,
                thread_summary_documenets,
            ) = self.prepare_channel_summaries(
                thread_summaries,
                self._build_channel_prompt(),
            )
            (
                daily_summaries,
                channel_summary_documenets,
            ) = self.prepare_daily_summaries(
                channel_summaries,
                self._build_daily_prompt(),
            )
            daily_summary_documents = (
                self.discord_summary_transformer.transform_daily_summary_to_document(
                    daily_summaries
                )
            )
        else:
            logging.info(f"No data received after the data: {from_date}")
            thread_summary_documenets = []
            channel_summary_documenets = []
            daily_summary_documents = []

        return (
            thread_summary_documenets,
            channel_summary_documenets,
            daily_summary_documents,
        )

    def stream_summary_documents(
        self,
        guild_id: str,
        selected_channels: list[str],
        from_date: datetime,
        batch_size: int = 50,
    ) -> Iterator[list[Document]]:
        """
        Stream summary documents in sorted batches while preparing them.

        Parameters
        ----------
        guild_id : str
            The guild id to access data
        selected_channels : list[str]
            The discord channels to produce summaries
        from_date : datetime
            Get the raw data from a specific date
        batch_size : int
            Number of documents per yielded batch

        Yields
        ------
        list[llama_index.Document]
            A batch of documents sorted by date
        """
        raw_data_grouped = prepare_grouped_data(
            guild_id, from_date, selected_channels
        )

        if raw_data_grouped == {}:
            logging.info(f"No data received after the data: {from_date}")
            return

        # Process per day to avoid building the entire set in memory
        buffer: list[Document] = []

        for date_key in sorted(raw_data_grouped.keys()):
            # Prepare thread summaries for a single day
            thread_summaries = self.prepare_thread_summaries(
                guild_id,
                {date_key: raw_data_grouped[date_key]},
                self._build_thread_prompt(),
            )

            # Prepare channel summaries and collect thread documents
            (
                channel_summaries,
                thread_summary_documenets,
            ) = self.prepare_channel_summaries(
                thread_summaries,
                self._build_channel_prompt(),
            )

            # Prepare daily summaries and collect channel documents
            (
                daily_summaries,
                channel_summary_documenets,
            ) = self.prepare_daily_summaries(
                channel_summaries,
                self._build_daily_prompt(),
            )

            # Convert daily summaries to documents
            daily_summary_documents = (
                self.discord_summary_transformer.transform_daily_summary_to_document(
                    daily_summaries
                )
            )

            # Collect and sort by date inside the day (identical dates but keep consistency)
            day_docs: list[Document] = (
                thread_summary_documenets
                + channel_summary_documenets
                + daily_summary_documents
            )
            day_docs.sort(key=lambda d: d.metadata["date"])

            buffer.extend(day_docs)

            # Yield full batches
            while len(buffer) >= batch_size:
                batch = buffer[:batch_size]
                batch.sort(
                    key=lambda d: d.metadata["date"]  # type: ignore
                )
                yield batch
                buffer = buffer[batch_size:]

        # Yield any remaining documents
        if buffer:
            buffer.sort(
                key=lambda d: d.metadata["date"]  # type: ignore
            )
            yield buffer
