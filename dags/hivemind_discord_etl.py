import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from dotenv import load_dotenv
from hivemind_etl_helpers.discord_mongo_summary_etl import process_discord_summaries
from hivemind_etl_helpers.discord_mongo_vector_store_etl import (
    process_discord_guild_mongo,
)
from hivemind_etl_helpers.src.utils.modules import ModulesDiscord
from llama_index.core import Settings
from llama_index.llms.openai import OpenAI

with DAG(
    dag_id="discord_vector_store_update",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
) as dag:

    @task
    def get_discord_communities(**kwargs) -> list[dict[str, str | datetime | list]]:
        """
        Getting all communities having discord from database
        """
        from_start = kwargs["dag_run"].conf.get("from_start", False)
        logging.info(f"From start: {from_start}")

        communities = ModulesDiscord().get_learning_platforms()

        if from_start:
            for community in communities:
                community["from_start"] = from_start

        return communities

    @task
    def start_discord_vectorstore(community_info: dict[str, str | datetime | list]):
        load_dotenv()
        community_id = community_info["community_id"]
        platform_id = community_info["platform_id"]
        selected_channels = community_info["selected_channels"]
        from_date = community_info["from_date"]
        from_start = community_info.get("from_start", False)

        Settings.llm = OpenAI(model="gpt-4o-mini-2024-07-18")

        logging.info(
            f"Processing community_id: {community_id} | platform_id: {platform_id}"
        )
        process_discord_guild_mongo(
            community_id=community_id,
            platform_id=platform_id,
            selected_channels=selected_channels,
            default_from_date=from_date,
            from_start=from_start,
        )
        logging.info(
            f"Community {community_id} Job finished | platform_id: {platform_id}"
        )

    communities_info = get_discord_communities()
    # `start_discord_vectorstore` will be called multiple times
    # with the length of the list
    start_discord_vectorstore.expand(community_info=communities_info)


with DAG(
    dag_id="discord_summary_vector_store",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
) as dag:

    @task
    def get_mongo_discord_communities(**kwargs) -> list[dict[str, str | datetime | list]]:
        """
        Getting all communities having discord from database
        this function is the same with `get_discord_communities`
        we just changed the name for the pylint
        """
        from_start = kwargs["dag_run"].conf.get("from_start", False)
        logging.info(f"From start: {from_start}")

        communities = ModulesDiscord().get_learning_platforms()

        if from_start:
            for community in communities:
                community["from_start"] = from_start

        return communities

    @task
    def start_discord_summary_vectorstore(
        community_info: dict[str, str | datetime | list]
    ):
        load_dotenv()

        community_id = community_info["community_id"]
        platform_id = community_info["platform_id"]
        selected_channels = community_info["selected_channels"]
        from_date = community_info["from_date"]
        from_start = community_info.get("from_start", False)

        Settings.llm = OpenAI(model="gpt-4o-mini-2024-07-18")

        logging.info(
            f"Working on community, {community_id}| platform_id: {platform_id}"
        )
        process_discord_summaries(
            community_id=community_id,
            platform_id=platform_id,
            selected_channels=selected_channels,
            default_from_date=from_date,
            from_start=from_start,
            verbose=False,
        )
        logging.info(
            f"Community {community_id} Job finished | platform_id: {platform_id}"
        )

    communities = get_mongo_discord_communities()
    start_discord_summary_vectorstore.expand(community_info=communities)
