# type: ignore
# remove the above when all tasks were filled
import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from analyzer_helper.common.fetch_platforms import FetchPlatforms
from analyzer_helper.common.load_transformed_data import LoadTransformedData
from analyzer_helper.discourse.analyzer import Analyzer
from analyzer_helper.discourse.extract_raw_info import ExtractRawInfo
from analyzer_helper.discourse.transform_raw_info import TransformRawInfo

with DAG(
    dag_id="discourse_analyzer_etl",
    start_date=datetime(2024, 5, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
) as dag:

    @task
    def fetch_discourse_platforms(**kwargs) -> list[dict[str, str | datetime | bool]]:
        """
        fetch discourse platforms

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
        fetcher = FetchPlatforms(plaform_name='discourse') #TODO: Modify accordingly to `discourse` needs

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
    def discourse_etl_raw_data(
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
                'forum_endpoint' : str,
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
        forum_endpoint = platform_info["forum_endpoint"] #TODO: Understand with Amin if this is appropriately reflecting current structure
        period = platform_info["period"]
        recompute = platform_info["recompute"]
        # If recompute is False, then just extract from the latest saved document
        # within rawmemberactivities collection using their date
        # else, just extract from the `period`
        extractor = ExtractRawInfo(forum_endpoint=forum_endpoint)
        extracted_data = extractor.extract(period=period, recompute=recompute)
        transformer = TransformRawInfo()
        transformed_data = transformer.transform(
            raw_data=extracted_data,
        )
        # if recompute is True, then replace the whole previously saved data in
        # database with the new ones
        # else, just save the new ones
        loader = LoadTransformedData(platform_id=platform_id)
        loader.load(processed_data=transformed_data, recompute=recompute)

    @task
    def discourse_etl_raw_members(
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
                'forum_endpoint' : str,
                'period' : datetime,
                'recompute': bool,
            }
            ```
        """
        platform_id = platform_info["platform_id"]
        forum_endpoint = platform_info["forum_endpoint"] #TODO: Understand with Amin if this is appropriately reflecting current structure
        period = platform_info["period"]
        recompute = platform_info["recompute"]
        # if recompute was false, then will fetch from the previously saved data date
        # else, then will fetch all platform's members data
        extractor = ExtractRawMembers(forum_endpoint=forum_endpoint, platform_id=platform_id)
        extracted_data = extractor.extract(period=period, recompute=recompute)
        transformer = TransformRawMembers(platform_id=platform_id)
        transformed_data = transformer.transform(
            raw_data=extracted_data, platform_id=platform_id
        )
        loader = LoadTransformedMembers(platform_id=platform_id)
        loader.load(processed_data=transformed_data, recompute=recompute)

    @task
    def analyze_discourse(platform_processed: dict[str, str | bool]) -> None:
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
        fetcher = FetchPlatforms(plaform_name='discourse')
        platform_id = platform_processed["platform_id"]
        recompute = platform_processed["recompute"]

        platform_data = fetcher.fetch_analyzer_parameters(platform_id)

        metadata = platform_data["metadata"]
        period = metadata["period"]
        action = metadata["action"]
        window = metadata["window"]
        channels = metadata["selectedChannels"]

        analyzer = Analyzer(
            platform_id=platform_id,
            channels=channels,
            period=period,
            action=action,
            window=window,
        )
        analyzer.analyze(recompute=recompute)

    platform_modules = fetch_discourse_platforms()

    raw_data_etl = discourse_etl_raw_data.expand(platform_info=platform_modules)
    raw_members_etl = discourse_etl_raw_members.expand(platform_info=platform_modules)

    raw_members_etl >> analyze_discourse(platform_processed=raw_data_etl)