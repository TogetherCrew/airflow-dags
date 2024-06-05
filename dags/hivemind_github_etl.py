from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from hivemind_etl_helpers.github_etl import process_github_vectorstore
from hivemind_etl_helpers.src.utils.modules import ModulesGitHub

with DAG(
    dag_id="github_vector_store",
    start_date=datetime(2024, 2, 21),
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    @task
    def get_github_communities():
        github_communities = ModulesGitHub().get_learning_platforms()
        return github_communities

    @task
    def process_github_community(
        community_information: dict[str, str | datetime | list[str] | None]
    ):
        community_id: str = community_information["community_id"]  # type: ignore
        organization_ids: list[str] = community_information.get("organization_ids", [])  # type: ignore
        repo_ids: list[str] = community_information.get("repo_ids", [])  # type: ignore
        from_date: datetime | None = community_information["from_date"]  # type: ignore

        logging.info(f"Starting Github ETL | community_id: {community_id}")
        process_github_vectorstore(
            community_id=community_id,
            github_org_ids=organization_ids,
            repo_ids=repo_ids,
            from_starting_date=from_date,
        )

    communities_info = get_github_communities()
    process_github_community.expand(community_information=communities_info)
