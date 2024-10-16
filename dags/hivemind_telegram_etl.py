import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from dotenv import load_dotenv
from llama_index.core import Document
from hivemind_etl_helpers.ingestion_pipeline import CustomIngestionPipeline
from hivemind_etl_helpers.src.db.telegram import TelegramChats

with DAG(
    dag_id="telegram_vector_store",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 3 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    @task
    def fetch_chat_ids() -> list[str]:
        """
        Getting all communities having discord from database

        Returns
        ---------
        chat_ids : list[str]
            a list of Telegram chat id
        """
        chat_ids = TelegramChats.extract_chat_ids()
        return chat_ids

    @task
    def chat_existance(self, chat_id: str) -> tuple[str, str]:
        """
        check if the community & platform was created for the telegram or not
        if not, create a community and platform and hivemind module for it
        else, just skip the work

        Parameters
        -----------
        chat_id : str
            a telegram chat id

        Returns
        ---------
        chat_id : str
            a telegram chat id
        community_id : str
            the community id, related the created community
        """
        pass

    @task
    def processor(self, chat_id: str, community_id: str) -> None:
        """
        extract, transform, and load telegram data

        Parameters
        -----------
        chat_id : str
            a telegram chat id
        community_id : str
            the community id, related the created community
        """
        pass
        # Extract

        # Transform
        documents: list[Document]
        # Load
        ingestion_pipeline = CustomIngestionPipeline(community_id=community_id, collection_name="telegram")
        ingestion_pipeline.run_pipeline(docs=documents)


    chat_ids = fetch_chat_ids()
    chat_id, community_id = chat_existance.expand(chat_id=chat_ids)
    processor(chat_id=chat_id, community_id=community_id)
