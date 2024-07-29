from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from hivemind_etl_helpers.notion_etl import NotionProcessor
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

        # separating each page/database id into one array
        # for better tracebility of errors
        community_data_flattened = []
        for community in communities:
            community_id = community["community_id"]
            access_token = community["access_token"]
            prepared_data = {
                "community_id": community_id,
                "access_token": access_token,
            }
            for page_id in community["page_ids"]:
                community_data_flattened.append(
                    {
                        **prepared_data,
                        "page_id": page_id,
                    }
                )
            for db_id in community["database_ids"]:
                community_data_flattened.append(
                    {
                        **prepared_data,
                        "database_id": db_id,
                    }
                )

        return community_data_flattened

    @task
    def start_notion_vectorstore(community_info: dict[str, str | list[str]]):
        community_id = community_info["community_id"]
        access_token = community_info["access_token"]
        database_id = communities_info.get("database_id", None)
        page_id = community_info.get("page_id", None)

        logging.info(f"Working on community, {community_id}")
        extractor = NotionProcessor(
            community_id=community_id,  # type: ignore
            access_token=access_token,  # type: ignore
        )
        if database_id is not None:
            extractor.process_database(
                database_id=database_id,  # type: ignore
            )
        elif page_id is not None:
            extractor.process_page(
                page_id=page_id,  # type: ignore
            )
        else:
            raise ValueError(
                "No page id or database id given, At least one should be available!"
            )

        logging.info(f"Community {community_id} Job finished!")

    communities_info = get_notion_communities()
    start_notion_vectorstore.expand(community_info=communities_info)
