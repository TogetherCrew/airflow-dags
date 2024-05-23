from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from hivemind_etl_helpers.mediawiki_etl import process_mediawiki_etl
from hivemind_etl_helpers.src.utils.modules import ModulesMediaWiki

with DAG(
    dag_id="mediawiki_vector_store_update",
    start_date=datetime(2024, 2, 21),
    schedule_interval="0 4 * * *",
) as dag:

    @task
    def get_mediawiki_communities() -> list[dict[str, str | list[str]]]:
        """
        Getting all communities having mediawiki from database
        """
        communities = ModulesMediaWiki().get_learning_platforms()
        return communities

    @task
    def start_mediawiki_vectorstore(community_info: dict[str, str | list[str]]):
        community_id = community_info["community_id"]
        page_titles = community_info["page_titles"]
        api_url = community_info["base_url"]

        logging.info(f"Working on community, {community_id}")
        process_mediawiki_etl(
            community_id=community_id,  # type: ignore
            page_titles=page_titles,  # type: ignore
            api_url=api_url,  # type: ignore
        )
        logging.info(f"Community {community_id} Job finished!")

    communities_info = get_mediawiki_communities()
    start_mediawiki_vectorstore.expand(community_info=communities_info)
