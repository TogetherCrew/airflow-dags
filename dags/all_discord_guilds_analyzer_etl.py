import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from analyzer_helper.common.analyzer import Analyzer
from analyzer_helper.discord.discord_extract_raw_infos import DiscordExtractRawInfos
from analyzer_helper.discord.discord_extract_raw_members import DiscordExtractRawMembers
from analyzer_helper.discord.discord_load_transformed_data import (
    DiscordLoadTransformedData,
)
from analyzer_helper.discord.discord_load_transformed_members import (
    DiscordLoadTransformedMembers,
)
from analyzer_helper.discord.discord_transform_raw_data import DiscordTransformRawData
from analyzer_helper.discord.discord_transform_raw_members import (
    DiscordTransformRawMembers,
)
from analyzer_helper.discord.fetch_discord_platforms import FetchDiscordPlatforms

with DAG(
    dag_id="all_discord_guilds_analyzer_etl",
    start_date=datetime(2024, 5, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
) as dag:

    @task
    def fetch_discord_platforms(**kwargs) -> list[dict[str, str | datetime | bool]]:
        """
        fetch discord platforms

        Returns
        ---------
        platform_modules : list[dict[str, str | datetime | bool]]
            a list of data for each platform
            each platform's module information would have the information below
            ```
            {
                'platform_id' : str,
                'period': datetime,
                'guild_id' : str,
                'recompute': bool,  # default is False
            }
            ```

        """
        # the platform that needs to be recomputed
        platform_id_recompute = kwargs["dag_run"].conf.get(  # noqa: F841
            "recompute_platform", None
        )
        # for default we're setting the recompute for all platforms to False
        # if an id for `recompute_platform` was given
        # then just run the ETL job for that platform with `recompute = True`
        # meaning the return would be a list with just one platform information
        fetcher = FetchDiscordPlatforms()

        platforms = fetcher.fetch_all()

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
    def discord_etl_raw_data(
        platform_info: dict[str, str | datetime | bool]
    ) -> dict[str, str | list[dict] | bool]:
        """
        process one platform's data

        Parameters
        -----------
        platform_info : dict[str, str | datetime | bool]
            the information for extracting raw data
            the data should be something like this
            ```
            {
                'platform_id' : datetime,
                'guild_id' : str,
                'period' : datetime,
                'recompute': bool,
            }
            ```
            if recompute was false, then fetch from the previously saved data date


        Returns
        --------
        platform_processed : dict[str, str | bool]
            the platform that their data was processed
            ```
            {
                platform_id: str,
                recompute : bool,
            }
            ```
        """
        platform_id = platform_info["platform_id"]
        guild_id = platform_info["guild_id"]
        period = platform_info["period"]
        recompute = platform_info["recompute"]
        logging.info(
            f"PROCESSING PLATFORM ID: {platform_id}, GUILD_ID: {guild_id}, "
            f"PERIOD: {period}, RECOMPUTE: {recompute}"
        )
        # If recompute is False, then just extract from the latest saved document
        # within rawmemberactivities collection using their date
        # else, just extract from the `period`
        logging.info("Extracting raw discord data!")
        extractor = DiscordExtractRawInfos(guild_id=guild_id, platform_id=platform_id)
        extracted_data = extractor.extract(period=period, recompute=recompute)

        logging.info(f"{len(extracted_data)} raw data extracted!")

        logging.info("Transforming data to general data structure!")
        transformer = DiscordTransformRawData(
            platform_id=platform_id, guild_id=guild_id
        )
        transformed_data = transformer.transform(
            raw_data=extracted_data,
            platform_id=platform_id,
        )
        # if recompute is True, then replace the whole previously saved data in
        # database with the new ones
        # else, just save the new ones
        if len(transformed_data) != 0:
            logging.info("Loading Transformed data in database!")
            loader = DiscordLoadTransformedData(platform_id=platform_id)
            loader.load(processed_data=transformed_data, recompute=recompute)
        else:
            logging.info("No new data to load!")

    @task
    def discord_etl_raw_members(
        platform_info: dict[str, str | datetime | bool]
    ) -> None:
        """
        extract raw members data for a platform

        Parameters
        -----------
        platform_info : dict[str, str | datetime | bool]
            the information for extracting raw members data
            the data should be something like this
            ```
            {
                'platform_id' : datetime,
                'guild_id' : str,
                'period' : datetime,
                'recompute': bool,
            }
            ```
        """
        platform_id = platform_info["platform_id"]
        guild_id = platform_info["guild_id"]
        period = platform_info["period"]
        recompute = platform_info["recompute"]

        logging.info(
            f"PROCESSING PLATFORM ID: {platform_id}, GUILD_ID: {guild_id}, "
            f"PERIOD: {period}, RECOMPUTE: {recompute}"
        )
        # if recompute was false, then will fetch from the previously saved data date
        # else, then will fetch all platform's members data
        logging.info("Extracting Raw members!")
        extractor = DiscordExtractRawMembers(guild_id=guild_id, platform_id=platform_id)
        extracted_data = extractor.extract(recompute=recompute)

        logging.info(f"{len(extracted_data)} raw members extracted!")

        logging.info("Transforming raw members!")
        transformer = DiscordTransformRawMembers()
        transformed_data = transformer.transform(raw_members=extracted_data)

        if len(transformed_data) != 0:
            logging.info("Loading processed raw members!")
            loader = DiscordLoadTransformedMembers(platform_id=platform_id)
            loader.load(processed_data=transformed_data, recompute=recompute)
        else:
            logging.info("No new data to load!")

    @task(trigger_rule=TriggerRule.NONE_SKIPPED)
    def analyze_discord(platform_processed: dict[str, str | bool]) -> None:
        """
        start the analyzer to process data

        Parameters
        ------------
        platform_processed : dict[str, str | bool]
            the platform data to analyze
            value structure
            ```
            {
                platform_id: str,
                recompute : bool,
            }
            ```
        """
        logging.info(f"platform_processed: {platform_processed}")
        fetcher = FetchDiscordPlatforms()
        platform_id = platform_processed["platform_id"]
        recompute = platform_processed["recompute"]

        platform_data = fetcher.fetch_analyzer_parameters(platform_id)

        metadata = platform_data["metadata"]
        period = metadata["period"]
        action = metadata["action"]
        window = metadata["window"]
        resources = metadata["selectedChannels"]

        analyzer = Analyzer()
        analyzer.analyze(
            platform_id=platform_id,
            resources=resources,
            period=period,
            action=action,
            window=window,
            recompute=recompute,
        )

    platform_modules = fetch_discord_platforms()

    raw_data_etl = discord_etl_raw_data.expand(platform_info=platform_modules)
    raw_members_etl = discord_etl_raw_members.expand(platform_info=platform_modules)

    analyze_discord_task = analyze_discord.expand(platform_processed=platform_modules)

    [raw_data_etl, raw_members_etl] >> analyze_discord_task
