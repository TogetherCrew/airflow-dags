import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from analyzer_helper.common.analyzer import Analyzer
from analyzer_helper.common.fetch_platforms import FetchPlatforms
from analyzer_helper.common.load_transformed_data import LoadTransformedData
from analyzer_helper.common.load_transformed_members import LoadTransformedMembers
from analyzer_helper.discourse.extract_raw_data import ExtractRawInfo
from analyzer_helper.discourse.extract_raw_members import ExtractRawMembers
from analyzer_helper.discourse.fetch_categories import FetchDiscourseCategories
from analyzer_helper.discourse.transform_raw_data import TransformRawInfo
from analyzer_helper.discourse.transform_raw_members import TransformRawMembers
from tc_analyzer_lib.schemas.platform_configs import DiscourseAnalyzerConfig

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
                'id' : str,   # forum_endpoint
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
        fetcher = FetchPlatforms(
            platform_name="discourse",
        )

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
                'id' : str,
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
        forum_endpoint = platform_info["id"]
        period = platform_info["period"]
        recompute = platform_info["recompute"]

        extractor = ExtractRawInfo(
            forum_endpoint=forum_endpoint, platform_id=platform_id
        )
        extracted_data = extractor.extract(period=period, recompute=recompute)
        transformer = TransformRawInfo(forum_endpoint=forum_endpoint)
        transformed_data = transformer.transform(
            raw_data=extracted_data,
        )
        if len(transformed_data) != 0:
            logging.info(f"Loading {len(transformed_data)} transformed document in db!")
            loader = LoadTransformedData(platform_id=platform_id)
            loader.load(processed_data=transformed_data, recompute=recompute)
        else:
            logging.warning("No new document to load for discourse!")

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
        forum_endpoint = platform_info["id"]
        # period = platform_info["period"]
        recompute = platform_info["recompute"]

        extractor = ExtractRawMembers(
            forum_endpoint=forum_endpoint, platform_id=platform_id
        )
        extracted_data = extractor.extract(recompute=recompute)
        transformer = TransformRawMembers(endpoint=forum_endpoint)
        transformed_data = transformer.transform(raw_members=extracted_data)
        if len(transformed_data) != 0:
            logging.info(f"Loading {len(transformed_data)} transformed document in db!")
            loader = LoadTransformedMembers(platform_id=platform_id)
            loader.load(processed_data=transformed_data, recompute=recompute)
        else:
            logging.warning("No new document to load for discourse!")

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
        fetcher = FetchPlatforms(platform_name="discourse")
        platform_id = platform_processed["platform_id"]
        recompute = platform_processed["recompute"]
        forum_endpoint = platform_processed["id"]

        platform_data = fetcher.fetch_analyzer_parameters(platform_id)

        period = platform_data["period"]
        action = platform_data["action"]
        window = platform_data["window"]
        resources = platform_data["resources"]

        # in case no resource given, it means to process all
        if len(resources) == 0:
            category_fetcher = FetchDiscourseCategories(endpoint=forum_endpoint)
            resources = category_fetcher.fetch_all()

        analyzer = Analyzer()

        analyzer.analyze(
            platform_id=platform_id,
            resources=resources,
            period=period,
            action=action,
            window=window,
            recompute=recompute,
            config=DiscourseAnalyzerConfig(),
        )

    platform_modules = fetch_discourse_platforms()

    raw_data_etl = discourse_etl_raw_data.expand(platform_info=platform_modules)
    raw_members_etl = discourse_etl_raw_members.expand(platform_info=platform_modules)

    analyze_discourse_task = analyze_discourse.expand(
        platform_processed=platform_modules
    )

    [raw_data_etl, raw_members_etl] >> analyze_discourse_task
