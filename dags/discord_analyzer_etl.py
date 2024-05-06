# type: ignore
# remove the above when all tasks were filled
import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="discord_analyzer_etl",
    start_date=datetime(2024, 5, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
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

        # TODO
        pass

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
        # TODO: EXTRACT
        # If recompute is False, then just extract from the latest saved document
        # within rawmemberactivities collection using their date
        # else, just extract from the `period`

        # TODO: TRANSFORM

        # TODO: LOAD
        # if recompute is True, then replace the whole previously saved data in
        # database with the new ones
        # else, just save the new ones

        pass

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
        # TODO: EXTRACT
        # if recompute was false, then will fetch from the previously saved data date
        # else, then will fetch all platform's members data

        # TODO: TRANSFORM

        # TODO: LOAD
        pass

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
        # TODO
        pass

    platform_modules = fetch_discord_platforms()
    
    raw_data_etl = discord_etl_raw_data.expand(platform_info=platform_modules)
    raw_members_etl = discord_etl_raw_members.expand(platform_info=platform_modules)

    raw_members_etl >> analyze_discord(platform_processed=raw_data_etl)
