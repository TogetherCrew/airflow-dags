import logging
from datetime import datetime, timedelta
from typing import Literal

from airflow import DAG
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from dotenv import load_dotenv
from hivemind_etl_helpers.src.db.telegram.extract import (
    ExtractMessages,
    ExtractMessagesDaily,
    TelegramChats,
)
from hivemind_etl_helpers.src.db.telegram.transform import (
    SummarizeMessages,
    TransformMessages,
    TransformSummary,
)
from hivemind_etl_helpers.src.db.telegram.utils import TelegramModules, TelegramPlatform
from qdrant_client.http import models
from tc_hivemind_backend.ingest_qdrant import CustomIngestionPipeline

# Common DAG configuration
default_args = {
    "start_date": datetime(2024, 1, 1),
    "schedule_interval": "0 */4 * * *",
    "catchup": False,
    "max_active_runs": 1,
}


def create_telegram_dag(dag_type: Literal["messages", "summaries"]) -> DAG:
    """
    Factory function to create Telegram DAGs with shared task structure.

    Parameters
    ----------
    dag_type : Literal["messages", "summaries"]
        Type of DAG to create - either for processing messages or summaries

    Returns
    -------
    DAG
        Configured Airflow DAG
    """
    dag_id = f"telegram_{'vector_store' if dag_type == 'messages' else 'summary_vector_store'}"

    with DAG(dag_id=dag_id, **default_args) as dag:

        @task
        def fetch_chat_ids() -> list[tuple[int, str]]:
            """Get all Telegram chats from the database."""
            load_dotenv()
            return TelegramChats().extract_chats()

        @task
        def chat_existence(
            chat_info: tuple[str, str]
        ) -> dict[str, tuple[str, str] | str]:
            """Check and create community & platform for Telegram if needed."""
            chat_id, chat_name = chat_info

            platform_utils = TelegramPlatform(chat_id=chat_id, chat_name=chat_name)
            community_id, platform_id = platform_utils.check_platform_existence()
            if community_id is None:
                raise ValueError(
                    f"Telegram platform with chat_id: {chat_id} doesn't exist!"
                )

            modules = TelegramModules(community_id, platform_id)
            modules.create()

            return {
                "chat_info": chat_info,
                "community_id": str(community_id),
                "platform_id": str(platform_id),
            }

        @task(trigger_rule=TriggerRule.NONE_SKIPPED)
        def processor(details: dict[str, tuple[str, str] | str]) -> None:
            """Extract, transform, and load telegram data."""
            load_dotenv()
            logging.info(f"received details: {details}!")

            chat_info = details["chat_info"]
            community_id = details["community_id"]
            platform_id = details["platform_id"]
            chat_id, chat_name = chat_info

            logging.info(f"Started processing community: {community_id}")

            # Configure pipeline based on DAG type
            if dag_type == "messages":
                extractor = ExtractMessages(chat_id=chat_id)
                transformer = TransformMessages(chat_id=chat_id, chat_name=chat_name)
                collection_name = platform_id
                date_field = "createdAt"
                date_schema = models.PayloadSchemaType.FLOAT

                def process_data(messages):
                    return transformer.transform(messages=messages)

            else:  # summaries
                extractor = ExtractMessagesDaily(chat_id=chat_id)
                summarizer = SummarizeMessages(chat_id=chat_id, chat_name=chat_name)
                transformer = TransformSummary()
                collection_name = f"{platform_id}_summary"
                date_field = "date"
                date_schema = models.PayloadSchemaType.DATETIME

                def process_data(messages):
                    summaries = summarizer.summarize_daily(messages=messages)
                    return transformer.transform(summaries=summaries)

            # Set up ingestion pipeline
            ingestion_pipeline = CustomIngestionPipeline(
                community_id=community_id, collection_name=collection_name
            )

            # Get latest date and handle extraction
            latest_date = ingestion_pipeline.get_latest_document_date(
                field_name=date_field,
                field_schema=date_schema,
            )

            if latest_date and dag_type == "messages":
                # For messages, look back 30 days to catch edits
                from_date = latest_date - timedelta(days=30)
                logging.info(f"Started extracting from date: {from_date}!")
                messages = extractor.extract(from_date=from_date)
            else:
                if dag_type == "messages":
                    logging.info("Started extracting data from scratch!")
                    messages = extractor.extract()
                else:
                    # replacing the latest date with the day before to avoid missing any data
                    # in case real time extraction is needed
                    latest_date_day_before = latest_date - timedelta(days=1)
                    logging.info(
                        f"Started extracting from date: {latest_date_day_before}!"
                    )
                    messages = extractor.extract(from_date=latest_date_day_before)

            if dag_type == "messages":
                msg_count = len(messages)
            else:
                msg_count = len(sum(messages.values(), []))

            logging.info(f"Extracted {msg_count} messages!")

            # Process and load data
            documents = process_data(messages)
            logging.info(f"Transformed {len(documents)} messages!")

            ingestion_pipeline.run_pipeline(docs=documents)
            logging.info("Finished loading into database!")

        # Set up task dependencies
        chat_infos = fetch_chat_ids()
        details = chat_existence.expand(chat_info=chat_infos)
        processor.expand(details=details)

        return dag


# Create both DAGs
telegram_messages_dag = create_telegram_dag("messages")
telegram_summaries_dag = create_telegram_dag("summaries")
