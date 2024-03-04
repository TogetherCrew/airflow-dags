#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Example DAG demonstrating the usage of dynamic task mapping."""
from __future__ import annotations

import logging
from datetime import datetime

# import phoenix as px
from airflow import DAG
from airflow.decorators import task
from dotenv import load_dotenv
from hivemind_etl_helpers.discord_mongo_summary_etl import process_discord_summaries
from hivemind_etl_helpers.discord_mongo_vector_store_etl import (
    process_discord_guild_mongo,
)
from hivemind_etl_helpers.discourse_summary_etl import process_discourse_summary
from hivemind_etl_helpers.discourse_vectorstore_etl import process_discourse_vectorstore
from hivemind_etl_helpers.github_etl import process_github_vectorstore
from hivemind_etl_helpers.src.utils.get_communities_data import (
    get_discourse_communities,
    get_github_communities_data,
)
from hivemind_etl_helpers.src.utils.get_mongo_discord_communities import (
    get_all_discord_communities,
)

with DAG(
    dag_id="discord_vector_store_update",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
) as dag:

    @task
    def get_discord_communities() -> list[str]:
        """
        Getting all communities having discord from database
        """
        communities = get_all_discord_communities()
        return communities

    @task
    def start_discord_vectorstore(community_id: str):
        load_dotenv()
        logging.info(f"Working on community, {community_id}")
        process_discord_guild_mongo(community_id=community_id)
        logging.info(f"Community {community_id} Job finished!")

    communities = get_discord_communities()
    # `start_discord_vectorstore` will be called multiple times
    # with the length of the list
    start_discord_vectorstore.expand(community_id=communities)

with DAG(
    dag_id="discord_summary_vector_store",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",
) as dag:

    @task
    def get_mongo_discord_communities() -> list[str]:
        """
        Getting all communities having discord from database
        this function is the same with `get_discord_communities`
        we just changed the name for the pylint
        """
        communities = get_all_discord_communities()
        return communities

    @task
    def start_discord_summary_vectorstore(community_id: str):
        load_dotenv()
        logging.info(f"Working on community, {community_id}")
        process_discord_summaries(community_id=community_id, verbose=False)
        logging.info(f"Community {community_id} Job finished!")

    communities = get_mongo_discord_communities()
    start_discord_summary_vectorstore.expand(community_id=communities)


with DAG(
    dag_id="github_vector_store",
    start_date=datetime(2024, 2, 21),
    schedule_interval="0 2 * * *",
) as dag:

    def get_github_communities() -> list[str]:
        github_info = get_github_communities_data()
        # debugging
        # github_info = [{
        #     "community_id": "test_github",
        #     "organization_id": 133082471,
        #     "from_date": datetime(2024, 1, 1)
        # }]
        return github_info

    @task
    def process_github_community(community_information: dict[str, str | datetime]):
        community_id = community_information["community_id"]
        organization_id = community_information["organization_id"]
        from_date = community_information["from_date"]

        logging.info(f"Starting Github ETL | community_id: {community_id}")
        process_github_vectorstore(
            community_id=community_id,
            github_org_id=organization_id,
            from_starting_date=from_date,
        )

    communities_info = get_github_communities()
    process_github_community.expand(community_information=communities_info)


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
