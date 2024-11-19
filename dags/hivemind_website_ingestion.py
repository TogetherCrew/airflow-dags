import asyncio
import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from dotenv import load_dotenv
from hivemind_etl_helpers.src.utils.modules import ModulesWebsite
from hivemind_etl_helpers.website_etl import WebsiteETL

with DAG(
    dag_id="website_ingestion_embedding",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 3 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    @task
    def get_website_communities() -> list[dict[str, str | datetime | list]]:
        """
        Retrieve all communities with associated website URLs from the database.

        Returns:
            list[dict]: List of community information containing:
                - community_id (str)
                - platform_id (str)
                - urls (list)
        """
        communities = ModulesWebsite().get_learning_platforms()
        return communities

    @task
    def start_website_embedding(community_info: dict[str, str | datetime | list]):
        load_dotenv()
        community_id = community_info["community_id"]
        platform_id = community_info["platform_id"]
        urls = community_info["urls"]

        logging.info(
            f"Processing community_id: {community_id} | platform_id: {platform_id}"
        )
        website_etl = WebsiteETL(community_id=community_id)

        # Extract
        raw_data = asyncio.run(website_etl.extract(urls=urls))
        # transform
        documents = website_etl.transform(raw_data=raw_data)
        # load into db
        website_etl.load(documents=documents)

        logging.info(
            f"Community {community_id} Job finished | platform_id: {platform_id}"
        )

    communities_info = get_website_communities()
    start_website_embedding.expand(community_info=communities_info)
