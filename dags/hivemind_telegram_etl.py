import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from dotenv import load_dotenv
from hivemind_etl_helpers.ingestion_pipeline import CustomIngestionPipeline
from hivemind_etl_helpers.src.db.telegram.extract import TelegramChats, ExtractMessages
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
        Getting all communities having discord from database

        Returns
        ---------
        chat_info : list[tuple[str, str]]
            a list of Telegram chat name and id
        """
        chat_info = TelegramChats.extract_chats()
        return chat_info

    @task
    def chat_existance(chat_info: tuple[str, str]) -> tuple[str, str]:
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
        chat_id : str
            a telegram chat id
        community_id : str
            the community id, related the created community
        """
        chat_id = chat_info[0]
        chat_name = chat_info[1]

        utils = TelegramUtils(chat_id=chat_id, chat_name=chat_name)
        community_id = utils.check_platform_existance()
        if community_id is None:
            logging.info(
                f"Platform with chat_id: {chat_id} doesn't exist. "
                "Creating one instead!"
            )

            community_id = utils.create_platform()
    
        return chat_id, community_id

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
        messages = extractor.extract()

        transformer = TransformMessages(chat_id=chat_id, chat_name=chat_name)
        documents = transformer.transform(messages=messages)

        # Load
        ingestion_pipeline = CustomIngestionPipeline(community_id=community_id, collection_name="telegram")
        ingestion_pipeline.run_pipeline(docs=documents)

    chat_infos = fetch_chat_ids()
    chat_id, community_id = chat_existance.expand(chat_info=chat_infos)
    processor(chat_id=chat_id, community_id=community_id)
