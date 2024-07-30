import logging
from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.decorators import task
from violation_detection_helpers.modules import ViolationDetectionModules
from violation_detection_helpers import (
    ExtractPlatformRawData,
    TransformPlatformRawData,
    LoadPlatformLabeledData
)


with DAG(
    dag_id="violation_detection_etl",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    
    @task
    def get_violation_modules(**kwargs) -> list[dict[str, Any]]:
        platform_id_recompute = kwargs["dag_run"].conf.get(  # noqa: F841
            "recompute_platform", None
        )
        vd_module = ViolationDetectionModules()
        platforms = vd_module.retrieve_platforms(platform_name="discourse")

        # updating the recompute for a specific platform
        if platform_id_recompute:
            platforms = [
                platform
                for platform in platforms
                if platform["platform_id"] == platform_id_recompute
            ]
            for platform in platforms:
                platform["recompute"] = True

        return platforms
    
    @task
    def process_platforms(platform: dict[str, Any]):
        platform_id = platform["platform_id"]
        resources = platform["resources"]
        from_date = platform["from_date"]
        to_date = platform["to_date"]
        recompute = platform["recompute"]
        discord_users = platform["selected_discord_users"]
        
        # EXTRACT

        # TODO: Get resource_identifier from platform analyzer config
        # that we would have in future on database
        # For now it is discourse specific
        extractor = ExtractPlatformRawData(
            platform_id=platform_id,
            resource_identifier="category_id",
        )

        raw_data, override_data = extractor.extract(
            from_date=from_date,
            to_date=to_date,
            resources=resources,
            recompute=recompute,
        )

        # Transform

        transformer = TransformPlatformRawData()
        # TODO: prepare some report while transforming if recompute is False
        transformed_data = transformer.transform(raw_data)

        # Load

        loader = LoadPlatformLabeledData()
        loader.load(platform_id=platform, transformed_data=transformed_data)

        # message discord users
