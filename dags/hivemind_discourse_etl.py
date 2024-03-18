import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from hivemind_etl_helpers.discourse_summary_etl import process_discourse_summary
from hivemind_etl_helpers.discourse_vectorstore_etl import process_discourse_vectorstore
from hivemind_etl_helpers.src.utils.get_communities_data import (
    get_discourse_communities,
)

with DAG(
    dag_id="discourse_vector_store",
    start_date=datetime(2024, 3, 1),
    schedule_interval="0 2 * * *",
) as dag:

    @task
    def process_discourse_community(community_information: dict[str, str | datetime]):
        community_id = community_information["community_id"]
        forum_endpoint = community_information["endpoint"]
        from_date = community_information["from_date"]

        logging.info(f"Starting Discourse ETL | community_id: {community_id}")
        process_discourse_vectorstore(
            community_id=community_id,
            forum_endpoint=forum_endpoint,
            from_starting_date=from_date,
        )

    communities_info = get_discourse_communities()
    process_discourse_community.expand(community_information=communities_info)


with DAG(
    dag_id="discourse_summary_vector_store",
    start_date=datetime(2024, 2, 21),
    schedule_interval="0 2 * * *",
) as dag:

    @task
    def process_discourse_community_summary(
        community_information: dict[str, str | datetime]
    ):
        community_id = community_information["community_id"]
        forum_endpoint = community_information["endpoint"]
        from_date = community_information["from_date"]

        logging.info(f"Starting Discourse ETL | community_id: {community_id}")
        process_discourse_summary(
            community_id=community_id,
            forum_endpoint=forum_endpoint,
            from_starting_date=from_date,
        )

    communities_info = get_discourse_communities()
    process_discourse_community_summary.expand(community_information=communities_info)
