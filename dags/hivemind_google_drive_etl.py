from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from hivemind_etl_helpers.src.db.gdrive.gdrive_loader import GoogleDriveLoader
from hivemind_etl_helpers.src.utils.modules import ModulesGDrive
from tc_hivemind_backend.ingest_qdrant import CustomIngestionPipeline

with DAG(
    dag_id="gdrive_vector_store",
    start_date=datetime(2024, 2, 21),
    schedule_interval="0 4 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    @task
    def get_gdrive_communities():
        modules = ModulesGDrive()
        gdrive_communities = modules.get_learning_platforms()
        return gdrive_communities

    @task
    def process_gdrive_data(
        community_information: dict[str, str | list[str] | datetime | dict]
    ):
        community_id = community_information["community_id"]
        file_ids = community_information["file_ids"]
        folder_ids = community_information["folder_ids"]
        drive_ids = community_information["drive_ids"]
        refresh_token = community_information["refresh_token"]
        platform_id = community_information["platform_id"]

        logging.info(f"Starting Gdrive ETL | community_id: {community_id}")
        loader = GoogleDriveLoader(refresh_token=refresh_token)
        load_file_data = loader.load_data(
            file_ids=file_ids, folder_ids=folder_ids, drive_ids=drive_ids
        )

        ingest_data = CustomIngestionPipeline(
            community_id=community_id, collection_name=platform_id
        )
        ingest_data.run_pipeline(load_file_data)

    communities_info = get_gdrive_communities()
    process_gdrive_data.expand(community_information=communities_info)
