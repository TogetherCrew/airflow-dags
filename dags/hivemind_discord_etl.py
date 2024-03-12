import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from dotenv import load_dotenv
from hivemind_etl_helpers.discord_mongo_summary_etl import process_discord_summaries
from hivemind_etl_helpers.discord_mongo_vector_store_etl import (
    process_discord_guild_mongo,
)
from hivemind_etl_helpers.src.utils.mongo_discord_communities import (
    get_all_discord_communities,
)


with DAG(
    dag_id="discord_vector_store_update",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
) as dag:

    @task
    def get_discord_communities() -> list[str]:
        """
        Getting all communities having discord from database
        """
        communities = get_all_discord_communities()
        return communities

    @task
    def start_discord_vectorstore(community_id: str):
        load_dotenv()
        logging.info(f"Working on community, {community_id}")
        process_discord_guild_mongo(community_id=community_id)
        logging.info(f"Community {community_id} Job finished!")

    communities = get_discord_communities()
    # `start_discord_vectorstore` will be called multiple times
    # with the length of the list
    start_discord_vectorstore.expand(community_id=communities)


with DAG(
    dag_id="discord_summary_vector_store",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",
) as dag:

    @task
    def get_mongo_discord_communities() -> list[str]:
        """
        Getting all communities having discord from database
        this function is the same with `get_discord_communities`
        we just changed the name for the pylint
        """
        communities = get_all_discord_communities()
        return communities

    @task
    def start_discord_summary_vectorstore(community_id: str):
        load_dotenv()
        logging.info(f"Working on community, {community_id}")
        process_discord_summaries(community_id=community_id, verbose=False)
        logging.info(f"Community {community_id} Job finished!")

    communities = get_mongo_discord_communities()
    start_discord_summary_vectorstore.expand(community_id=communities)
