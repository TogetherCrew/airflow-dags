from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from dags.hivemind_etl_helpers.notion_etl import process_notion_etl
from hivemind_etl_helpers.src.utils.get_communities_data import (
    get_all_notion_communities,
)

with DAG(
    dag_id="notion_vector_store_update",
    start_date=datetime(2024, 2, 21),
    schedule_interval="0 4 * * *",
) as dag:

    @task
    def get_notion_communities() -> list[str]:
        """
        Getting all communities having notion from database
        """
        communities = get_all_notion_communities()
        return communities

    @task
    def start_notion_vectorstore(community_id: str):
        logging.info(f"Working on community, {community_id}")
        process_notion_etl(community_id=community_id)
        logging.info(f"Community {community_id} Job finished!")

    communities = get_notion_communities()
    start_notion_vectorstore.expand(community_id=communities)
