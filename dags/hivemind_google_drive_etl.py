from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from hivemind_etl_helpers.gdrive_ingestion_etl import GoogleDriveIngestionPipeline
from hivemind_etl_helpers.src.db.gdrive.gdrive_loader import GoogleDriveLoader
from hivemind_etl_helpers.src.utils.get_communities_data import (
    get_google_drive_communities,
)

with DAG(
    dag_id="gdrive_vector_store",
    start_date=datetime(2024, 2, 21),
    schedule_interval="0 4 * * *",
) as dag:

    @task
    def get_gdrive_communities():
        gdrive_communities = get_google_drive_communities()
        return gdrive_communities

    @task
    def process_gdrive_data(
        community_information: list[dict[str, str | list[str] | datetime | dict]]
    ):
        community_id = community_information["community_id"]
        file_ids = community_information["file_ids"]
        folder_ids = community_information["folder_ids"]
        drive_ids = community_information["drive_ids"]
        client_config = community_information['client_config']

        logging.info(f"Starting Gdrive ETL | community_id: {community_id}")
        loader = GoogleDriveLoader(client_config=client_config)
        load_file_data = loader.load_data(
            file_ids=file_ids, folder_ids=folder_ids, drive_ids=drive_ids
        )

        ingest_data = GoogleDriveIngestionPipeline(community_id=community_id)
        ingest_data.run_pipeline(load_file_data)

    communities_info = get_gdrive_communities()
    process_gdrive_data.expand(community_information=communities_info)
