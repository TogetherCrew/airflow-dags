import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from dotenv import load_dotenv
from hivemind_etl_helpers.ingestion_pipeline import CustomIngestionPipeline
from hivemind_etl_helpers.src.db.telegram.extract import ExtractMessages, TelegramChats
from hivemind_etl_helpers.src.db.telegram.transform import TransformMessages
from hivemind_etl_helpers.src.db.telegram.utils import TelegramModules, TelegramPlatform

with DAG(
    dag_id="telegram_vector_store",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 3 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    @task
    def fetch_chat_ids() -> list[tuple[int, str]]:
        """
        Getting all Telegram chats from the database

        Returns
        ---------
        chat_infos : list[tuple[int, str]]
            a list of Telegram chat id and name
        """
        load_dotenv()
        chat_infos = TelegramChats().extract_chats()
        return chat_infos

    @task
    def chat_existence(chat_info: tuple[str, str]) -> dict[str, tuple[str, str] | str]:
        """
        check if the community & platform was created for the telegram or not
        if not, create a community and platform and hivemind module for it
        else, just skip the work

        Parameters
        -----------
        chat_info : tuple[str, str]
            first index is chat_id and second is chat_name

        Returns
        ---------
        details : dict[str, tuple[str, str] | str]
            the chat details containing the chat_info
            and a community id related to that
            tuple containing telegram chat id and chat name
        """
        chat_id = chat_info[0]
        chat_name = chat_info[1]

        platform_utils = TelegramPlatform(chat_id=chat_id, chat_name=chat_name)
        community_id, platform_id = platform_utils.check_platform_existence()
        if community_id is None:
            logging.info(
                f"Platform with chat_id: {chat_id} doesn't exist. "
                "Creating one instead!"
            )

            community_id, platform_id = platform_utils.create_platform()

        modules = TelegramModules(community_id, platform_id)
        modules.create()

        return {
            "chat_info": chat_info,
            "community_id": str(community_id),
        }

    @task
    def processor(
        details: dict[str, tuple[str, str] | str],
    ) -> None:
        """
        extract, transform, and load telegram data

        Parameters
        -----------
        details : dict[str, tuple[str, str] | str]
            the chat details containing the chat_info
            and a community id related to that
            tuple containing telegram chat id and chat name
        """
        load_dotenv()
        logging.info(f"received details: {details}!")
        # unwrapping data
        chat_info = details["chat_info"]
        community_id = details["community_id"]

        logging.info(f"Started processing community: {community_id}")

        chat_id = chat_info[0]
        chat_name = chat_info[1]

        extractor = ExtractMessages(chat_id=chat_id)
        transformer = TransformMessages(chat_id=chat_id, chat_name=chat_name)
        ingestion_pipeline = CustomIngestionPipeline(
            community_id=community_id, collection_name="telegram"
        )

        latest_date = ingestion_pipeline.get_latest_document_date(
            field_name="createdAt"
        )

        if latest_date:
            # this is to catch any edits for messages of 30 days ago
            from_date = latest_date - timedelta(days=30)
            logging.info(f"Started extracting from date: {from_date}!")
            messages = extractor.extract(from_date=from_date)
        else:
            logging.info("Started extracting data from scratch!")
            messages = extractor.extract()

        logging.info(f"Extracted {len(messages)} messages!")
        documents = transformer.transform(messages=messages)
        logging.info(f"{len(messages)} Messages transformed!")
        ingestion_pipeline.run_pipeline(docs=documents)
        logging.info("Finished loading into database!")

    chat_infos = fetch_chat_ids()
    details = chat_existence.expand(chat_info=chat_infos)
    processor.expand(details=details)
