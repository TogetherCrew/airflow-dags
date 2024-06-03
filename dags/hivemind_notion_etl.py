from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from hivemind_etl_helpers.notion_etl import process_notion_etl
from hivemind_etl_helpers.src.utils.modules import ModulesNotion

with DAG(
    dag_id="notion_vector_store_update",
    start_date=datetime(2024, 2, 21),
    schedule_interval="0 4 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    @task
    def get_notion_communities() -> list[dict[str, str | list[str]]]:
        """
        Getting all communities having notion from database
        """
        communities = ModulesNotion().get_learning_platforms()
        return communities

    @task
    def start_notion_vectorstore(community_info: dict[str, str | list[str]]):
        community_id = community_info["community_id"]
        database_ids = community_info["database_ids"]
        page_ids = community_info["page_ids"]
        access_token = community_info["access_token"]

        logging.info(f"Working on community, {community_id}")
        process_notion_etl(
            community_id=community_id,  # type: ignore
            database_ids=database_ids,  # type: ignore
            page_ids=page_ids,  # type: ignore
            access_token=access_token,  # type: ignore
        )
        logging.info(f"Community {community_id} Job finished!")

    communities_info = get_notion_communities()
    start_notion_vectorstore.expand(community_info=communities_info)
