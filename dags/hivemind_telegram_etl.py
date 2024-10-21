import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from dotenv import load_dotenv
from hivemind_etl_helpers.ingestion_pipeline import CustomIngestionPipeline
from hivemind_etl_helpers.src.db.telegram.extract import ExtractMessages, TelegramChats
from hivemind_etl_helpers.src.db.telegram.transform import TransformMessages
from hivemind_etl_helpers.src.db.telegram.utility import TelegramUtils

with DAG(
    dag_id="telegram_vector_store",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 3 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    @task
    def fetch_chat_ids() -> list[tuple[str, str]]:
        """
        Getting all Telegram chats from the database

        Returns
        ---------
        chat_infos : list[tuple[str, str]]
            a list of Telegram chat id and name
        """
        load_dotenv()
        chat_infos = TelegramChats.extract_chats()
        logging.info(f"Extracted chats: {chat_infos}")
        return chat_infos

    @task
    def chat_existence(chat_info: tuple[str, str]) -> tuple[str, str]:
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
        chat_info : tuple[str, str]
            tuple containing telegram chat id and chat name
        """
        chat_id = chat_info[0]
        chat_name = chat_info[1]

        utils = TelegramUtils(chat_id=chat_id, chat_name=chat_name)
        community_id = utils.check_platform_existence()
        if community_id is None:
            logging.info(
                f"Platform with chat_id: {chat_id} doesn't exist. "
                "Creating one instead!"
            )

            community_id = utils.create_platform()

        return chat_info, community_id

    @task
    def processor(chat_info: tuple[str, str], community_id: str) -> None:
        """
        extract, transform, and load telegram data

        Parameters
        -----------
        chat_id : str
            a telegram chat id
        community_id : str
            the community id, related the created community
        """
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
            messages = extractor.extract(from_date=from_date)
        else:
            messages = extractor.extract()
        documents = transformer.transform(messages=messages)
        ingestion_pipeline.run_pipeline(docs=documents)

    chat_infos = fetch_chat_ids()
    chat_info, community_id = chat_existence.expand(chat_info=chat_infos)
    processor(chat_info=chat_info, community_id=community_id)
