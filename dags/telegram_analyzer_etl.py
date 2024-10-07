import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from analyzer_helper.common.analyzer import Analyzer
from analyzer_helper.common.fetch_platforms import FetchPlatforms
from analyzer_helper.common.load_transformed_data import LoadTransformedData
from analyzer_helper.common.load_transformed_members import LoadTransformedMembers
from analyzer_helper.telegram.extract_raw_data import ExtractRawInfo
from analyzer_helper.telegram.extract_raw_members import ExtractRawMembers
from analyzer_helper.telegram.transform_raw_data import TransformRawInfo
from analyzer_helper.telegram.transform_raw_members import TransformRawMembers

with DAG(
    dag_id="telegram_analyzer_etl",
    start_date=datetime(2024, 5, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
) as dag:

    @task
    def fetch_telegram_platforms(**kwargs) -> list[dict[str, str | datetime | bool]]:
        """
        fetch telegram platforms

        Returns
        ---------
        platform_modules : list[dict[str, str | datetime | bool]]
            a list of data for each platform
            each platform's module information would have the information below
            ```
            {
                'platform_id' : str,
                'period': datetime,
                'id' : str,   # chat_id
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
            platform_name="telegram",
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
    def telegram_etl_raw_data(
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
                'platform_id' : str,
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
        chat_id = platform_info["id"]
        period = platform_info["period"]
        recompute = platform_info["recompute"]

        logging.info(f"CHAT_ID: {chat_id}. Extracting raw data!")
        extractor = ExtractRawInfo(
            chat_id=chat_id,
            platform_id=platform_id,
        )
        extracted_data = extractor.extract(period=period, recompute=recompute)
        if len(extracted_data) != 0:
            logging.info(
                f"CHAT_ID: {chat_id}. {len(extracted_data)} data extracted! Transforming them . . ."
            )
            transformer = TransformRawInfo(chat_id=chat_id)
            transformed_data = transformer.transform(
                raw_data=extracted_data,
            )
            logging.info(
                f"CHAT_ID: {chat_id}. Loading {len(transformed_data)} to database!"
            )
            loader = LoadTransformedData(platform_id=platform_id)
            loader.load(processed_data=transformed_data, recompute=recompute)
        else:
            logging.info(f"CHAT_ID: {chat_id}. No raw data extracted!")

    @task
    def telegram_etl_raw_members(
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
                'platform_id' : str,
                'id' : str,
                'period' : datetime,
                'recompute': bool,
            }
            ```
        """
        platform_id = platform_info["platform_id"]
        chat_id = platform_info["id"]
        # period = platform_info["period"]
        recompute = platform_info["recompute"]

        logging.info(f"CHAT_ID: {chat_id}. Extracting raw members!")

        extractor = ExtractRawMembers(chat_id=chat_id, platform_id=platform_id)
        extracted_data = extractor.extract(recompute=recompute)
        if len(extracted_data) != 0:
            logging.info(
                f"CHAT_ID: {chat_id}. {len(extracted_data)} data extracted! Transforming them . . ."
            )
            transformer = TransformRawMembers()
            transformed_data = transformer.transform(raw_members=extracted_data)
            logging.info(
                f"CHAT_ID: {chat_id}. Loading {len(transformed_data)} to database!"
            )
            loader = LoadTransformedMembers(platform_id=platform_id)
            loader.load(processed_data=transformed_data, recompute=recompute)
        else:
            logging.info(f"CHAT_ID: {chat_id}. No raw members extracted!")

    @task
    def analyze_telegram(platform_processed: dict[str, str | bool]) -> None:
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
        fetcher = FetchPlatforms(platform_name="telegram")
        platform_id = platform_processed["platform_id"]
        recompute = platform_processed["recompute"]

        platform_data = fetcher.fetch_analyzer_parameters(platform_id)

        metadata = platform_data["metadata"]
        period = metadata["period"]
        action = metadata["action"]
        window = metadata["window"]
        resources = metadata["resources"]

        analyzer = Analyzer()

        analyzer.analyze(
            platform_id=platform_id,
            resources=resources,
            period=period,
            action=action,
            window=window,
            recompute=recompute,
        )

    platform_modules = fetch_telegram_platforms()

    raw_data_etl = telegram_etl_raw_data.expand(platform_info=platform_modules)
    raw_members_etl = telegram_etl_raw_members.expand(platform_info=platform_modules)

    analyze_telegram_task = analyze_telegram.expand(
        platform_processed=platform_modules
    )

    [raw_data_etl, raw_members_etl] >> analyze_telegram_task
