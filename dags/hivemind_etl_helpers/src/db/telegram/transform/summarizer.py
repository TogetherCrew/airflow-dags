from datetime import date, timedelta
import logging

from hivemind_etl_helpers.src.db.telegram.schema import TelegramMessagesModel
from hivemind_etl_helpers.src.db.telegram.transform import TransformMessages
from hivemind_etl_helpers.src.utils.summary.summary_base import SummaryBase
from llama_index.core import Settings
from llama_index.core.response_synthesizers.base import BaseSynthesizer
from llama_index.core.llms import MockLLM


class SummarizeMessages(SummaryBase):
    def __init__(
        self,
        chat_id: int,
        chat_name: str,
        response_synthesizer: BaseSynthesizer | None = None,
        verbose: bool = False,
        **kwargs,
    ) -> None:
        llm = kwargs.get("llm", Settings.llm)
        super().__init__(llm, response_synthesizer, verbose)

        if not isinstance(llm, MockLLM):
            logging.info(f"Using LLM for summaries: {llm.model}")
        else:
            logging.info("Mock LLM is enabled! No LLM will be used for summaries!")

        self.message_transformer = TransformMessages(
            chat_id=chat_id, chat_name=chat_name
        )

    def summarize_daily(
        self, messages: dict[date, list[TelegramMessagesModel]]
    ) -> dict[date, str]:
        """
        summarize the daily messages

        Parameters
        -----------
        messages : dict[date, list[TelegramMessagesModel]]
            daily grouped messages

        Returns
        ---------
        summaries : dict[date, str]
            the summaries of each group
        """
        summaries: dict[date, str] = {}

        # per each daily messages
        for day, msgs in messages.items():
            # if no messages were available
            if not msgs:
                continue

            start_date = day.strftime("%d/%m/%Y")
            end_date = (day + timedelta(days=1)).strftime("%d/%m/%Y")

            day_documents = self.message_transformer.transform(messages=msgs)
            summary = self._get_summary(
                messages_document=day_documents,
                summarization_query=(
                    "You are a social media summarizer. Given the raw messages below, produce a clear, de-duplicated bullet-point summary optimized for retrieval.\n\n"
                    "Guidelines:\n"
                    "- Merge overlapping/duplicate posts; remove noise, greetings, emojis, and links.\n"
                    "- Group by topic; surface consensus and notable disagreements.\n"
                    "- Capture concrete facts (who/what/when/where), decisions, outcomes, blockers, and action items.\n"
                    "- Note counts if many users mention the same thing (e.g., \"~12 mentions\").\n"
                    "- Resolve pronouns to canonical entities when obvious.\n"
                    "- Include important dates/times and hashtags only if they add meaning.\n"
                    "- No speculation; avoid quotes unless essential.\n\n"
                    "Output:\n"
                    "- 5-10 concise bullets (one line each), most important first.\n"
                    "- Each bullet starts with a bold topic tag in brackets, then the point.\n"
                    "- No preamble or conclusion.\n\n"
                    f"Summarize these Telegram group chat messages from {start_date} to {end_date}:"
                ),
            )

            summaries[day] = summary

        return summaries
