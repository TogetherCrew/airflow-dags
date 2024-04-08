from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from hivemind_etl_helpers.github_etl import process_github_vectorstore
from hivemind_etl_helpers.src.utils.get_communities_data import \
    get_github_communities_data

with DAG(
    dag_id="github_vector_store",
    start_date=datetime(2024, 2, 21),
    schedule_interval="0 2 * * *",
) as dag:

    @task
    def process_github_community(
        community_information: dict[str, str | datetime]
         ):
        community_id = community_information["community_id"]
        organization_id = community_information["organization_id"]
        from_date = community_information["from_date"]

        logging.info(f"Starting Github ETL | community_id: {community_id}")
        process_github_vectorstore(
            community_id=community_id,
            github_org_id=organization_id,
            from_starting_date=from_date,
        )

    communities_info = get_github_communities_data()
    process_github_community.expand(community_information=communities_info)
