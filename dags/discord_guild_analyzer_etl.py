import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
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
from dateutil.parser import parse

with DAG(
    dag_id="discord_guild_analyzer_etl",
    start_date=datetime(2024, 5, 1),
    schedule_interval=None,  # we would always run this manually
    catchup=False,
) as dag:
    """
    processing data just for one guild
    """

    @task
    def fetch_discord_platforms(**kwargs) -> dict[str, str | datetime | bool]:
        """
        fetch discord platforms

        Returns
        ---------
        platform_module : dict[str, str | datetime | bool]
            a platform's module information would have the information below
            ```
            {
                'platform_id' : str,
                'period': datetime,
                'guild_id' : str,
                'recompute': bool,  # default is False
            }
            ```

        """
        recompute = kwargs["dag_run"].conf.get("recompute", None)  # noqa: F841
        platform_id = kwargs["dag_run"].conf.get("platform_id", None)  # noqa: F841
        period = kwargs["dag_run"].conf.get("period", None)  # noqa: F841
        guild_id = kwargs["dag_run"].conf.get("guild_id", None)  # noqa: F841
        if recompute is None:
            raise AttributeError("recompute is not sent!")
        if platform_id is None:
            raise AttributeError("platform_id is not sent!")
        if period is None:
            raise AttributeError("period is not sent!")
        if guild_id is None:
            raise AttributeError("guild_id is not sent!")

        platform = {
            "recompute": recompute,
            "platform_id": platform_id,
            "period": parse(period),
            "guild_id": guild_id,
        }

        return platform

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

        logging.info("Transforming raw members!")
        transformer = DiscordTransformRawMembers()
        transformed_data = transformer.transform(
            raw_members=extracted_data,
        )

        if len(transformed_data) != 0:
            logging.info("Loading processed raw members!")
            loader = DiscordLoadTransformedMembers(platform_id=platform_id)
            loader.load(processed_data=transformed_data, recompute=recompute)
        else:
            logging.info("No new data to load!")

    @task
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

    raw_data_etl = discord_etl_raw_data(platform_info=platform_modules)
    raw_members_etl = discord_etl_raw_members(platform_info=platform_modules)

    analyze_discord_task = analyze_discord(platform_processed=platform_modules)

    [raw_data_etl, raw_members_etl] >> analyze_discord_task
