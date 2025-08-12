import logging

from hivemind_etl_helpers.src.db.discord.summary.summary_utils import (
    DiscordSummaryTransformer,
)
from hivemind_etl_helpers.src.db.discord.utils.transform_discord_raw_messges import (
    transform_discord_raw_messages,
)
from hivemind_etl_helpers.src.utils.summary.summary_base import SummaryBase
from llama_index.core import Document, Settings
from llama_index.core.response_synthesizers.base import BaseSynthesizer


class PrepareSummaries(SummaryBase):
    def __init__(
        self,
        response_synthesizer: BaseSynthesizer | None = None,
        verbose: bool = False,
        **kwargs,
    ) -> None:
        self.discord_summary_transformer = DiscordSummaryTransformer()
        llm = kwargs.get("llm", Settings.llm)
        logging.info(f"Using LLM for summaries: {llm.model}")

        super().__init__(
            llm=llm, response_synthesizer=response_synthesizer, verbose=verbose
        )
        # initialization
        self.prefix: str = ""

    def prepare_thread_summaries(
        self,
        guild_id: str,
        raw_data_grouped: dict[float, dict[str, dict[str | None, list]]],
        summarization_query: str,
    ) -> dict[float, dict[str, dict[str | None, str]]]:
        """
        prepare the summaries for threads

        Parameters
        -----------
        guild_id : str
            the guild id to convert the raw messages to documents
        raw_data_grouped : dict[float, dict[str, dict[str | None, list]]]
            the raw data grouped by date, channel and thread in third nesting level
        summarization_query : str
            the summarization query to do on the LLM

        Returns
        --------
        thread_summaries : dict[float, dict[str, dict[str | None, str]]]
            the summaries per date, channel, and thread
            the third level are the summaries saved
        """
        self.prefix = f"GUILDID: {guild_id} "
        logging.info(f"{self.prefix}Preparing the thread summaries")

        total_call_count = 0
        for date in raw_data_grouped.keys():
            for channel in raw_data_grouped[date].keys():
                total_call_count += len(raw_data_grouped[date][channel])

        idx = 1
        thread_summaries: dict[str, dict[str, dict[str | None, str]]] = {}
        for date in raw_data_grouped.keys():
            for channel in raw_data_grouped[date].keys():
                for thread in raw_data_grouped[date][channel].keys():
                    # raw messages of the thread
                    raw_msgs = raw_data_grouped[date][channel][thread]
                    logging.info(
                        f"{self.prefix} Summrizing threads {idx}/{total_call_count}"
                    )
                    idx += 1
                    messages_document = transform_discord_raw_messages(
                        guild_id=guild_id, messages=raw_msgs, exclude_metadata=True
                    )
                    summary_response = self._get_summary(
                        messages_document, summarization_query
                    )
                    thread_summaries.setdefault(date, {}).setdefault(
                        channel, {}
                    ).setdefault(thread, summary_response)

        return thread_summaries

    def prepare_channel_summaries(
        self,
        thread_summaries: dict[float, dict[str, dict[str | None, str]]],
        summarization_query: str,
    ) -> tuple[dict[float, dict[str, str]], list[Document]]:
        """
        prepare the daily channel summaries based on the thread summaries

        Parameters
        -----------
        thread_summaries : dict[float, dict[str, dict[str | None, str]]]
            the thread summaries per day and per channel
        summarization_query : str
            the summarization query to do on the LLM

        Returns
        ---------
        channel_summaries : dict[float, dict[str, str]]
            the summaries per day for different channel
        thread_summary_documenets : list[llama_index.Document]
            a list of documents related to channel summaries
        """
        logging.info(f"{self.prefix}Preparing the channel summaries")

        total_call_count = 0
        for date in thread_summaries.keys():
            total_call_count += len(thread_summaries[date].keys())

        thread_summary_documenets: list[Document] = []
        channel_summaries: dict[str, dict[str, str]] = {}

        idx = 1
        for date in thread_summaries.keys():
            for channel in thread_summaries[date].keys():
                channel_documents: list[Document] = []
                for thread in thread_summaries[date][channel].keys():
                    thread_summary = thread_summaries[date][channel][thread]
                    thread_summary_splitted = self.split_lines(thread_summary)

                    for summary in thread_summary_splitted:
                        thread_doc = self.discord_summary_transformer.transform_thread_summary_to_document(
                            thread_name=thread,
                            summary_date=date,
                            thread_summary=summary,
                            thread_channel=channel,
                        )
                        thread_summary_documenets.append(thread_doc)

                    # modifying the thread name just for channel summarizer
                    thread_doc_modified = self.discord_summary_transformer.transform_thread_summary_to_document(
                        thread_name="Main channel" if thread is None else thread,
                        summary_date=date,
                        thread_summary=thread_summary,
                        thread_channel=channel,
                    )
                    channel_documents.append(thread_doc_modified)

                logging.info(
                    f"{self.prefix} Summrizing channels {idx}/{total_call_count}"
                )
                idx += 1

                channel_summary: str
                # if we had multiple documents
                if len(channel_documents) != 1:
                    channel_summary = self._get_summary(
                        channel_documents, summarization_query
                    )
                # if just there was one thread
                else:
                    channel_summary = channel_documents[0].text

                channel_summaries.setdefault(date, {}).setdefault(
                    channel, channel_summary
                )

        return channel_summaries, thread_summary_documenets

    def prepare_daily_summaries(
        self,
        channel_summaries: dict[float, dict[str, str]],
        summarization_query: str,
    ) -> tuple[dict[str, str], list[Document]]:
        """
        prepare the daily summaries based on the channel summaries

        Parameters
        -----------
        channel_summaries : dict[float, dict[str, str]]
            the thread summaries per day, per channel
        summarization_query : str
            the summarization query to do on the LLM

        Returns
        ---------
        daily_summaries : dict[float, str]
            the summaries per day for different channel
        channel_summary_documenets : list[llama_index.Document]
            a list of documents related to the summaries of the channel
        """
        logging.info(f"{self.prefix}Preparing the daily summaries")
        channel_summary_documenets: list[Document] = []
        daily_summaries: dict[float, str] = {}

        total_call_count = len(channel_summaries.keys())

        idx = 1
        for date in channel_summaries.keys():
            daily_documents: list[Document] = []
            for channel in channel_summaries[date].keys():
                channel_doc = self.discord_summary_transformer.transform_channel_summary_to_document(
                    channel_name=channel,
                    channel_summary=channel_summaries[date][channel],
                    summary_date=date,
                )
                daily_documents.append(channel_doc)
                channel_summary_documenets.append(channel_doc)

            logging.info(f"{self.prefix} Summrizing daily {idx}/{total_call_count}")
            idx += 1

            day_summary: str
            if len(daily_documents) != 1:
                day_summary = self._get_summary(daily_documents, summarization_query)
            else:
                day_summary = daily_documents[0].text

            daily_summaries[date] = day_summary

        return daily_summaries, channel_summary_documenets

    def split_lines(
        self,
        text: str,
    ) -> list[str]:
        """
        split a text by its bullet points

        Parameters
        -----------
        text : str
            the text that might contain multiple bullet points

        Returns
        --------
        splitted_text : list[str]
            a list of strings which is the original text
            but splitted into multiple parts
        """
        splitted_text = text.split("\n")
        empty_string = ""
        while empty_string in splitted_text:
            splitted_text.remove(empty_string)

        return splitted_text
