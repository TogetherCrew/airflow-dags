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
        platform_id_recompute = kwargs["dag_run"].conf.get("recompute_platform", None)

        # for default we're setting the recompute for all platforms to False
        # if an id for `recompute_platform` was given
        # then just run the ETL job for that platform with `recompute = True`
        # meaning the return would be a list with just one platform information

        # TODO
        pass

    @task
    def discord_extract_raw_data(
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
        platform_raw_data : dict[str, str | list[str] | bool]
            the raw data for a platform
            example data
            ```
            {
                platform_id: str,
                raw_data : list[dict],
                recompute : bool,
            }
            ```
        """
        # TODO
        pass

    @task
    def discord_transform_raw_data(
        platform_raw_data: dict[str, str | list[dict] | bool]
    ) -> dict[str, str | list[dict] | bool]:
        """
        transform raw data given for a platform to a general data structure

        Parameters
        ------------
        platform_raw_data : dict[str, str | list[str] | bool]
            the raw data for a platform
            example data
            ```
            {
                platform_id: str,
                raw_data : list[dict],
                recompute : bool,
            }
            ```

        Returns
        ----------
        transformed_data : dict[str, str | list[dict] | bool]
            the platform's data transformed
            ```
            {
                platform_id: str,
                transformed_data : list[dict],
                recompute : bool,
            }
            ```
        """
        # TODO
        pass

    @task
    def discord_load_transformed_data(
        platform_transformed_data: dict[str, str | list[dict] | bool]
    ) -> dict[str, str | bool]:
        """
        load transformed data within database

        Parameters
        ------------
        platform_raw_data : dict[str, str | list[str] | bool]
            the raw data for a platform
            example data
            ```
            {
                platform_id: str,
                raw_data : list[dict],
                recompute : bool,
            }
            ```
            if recompute is True, then replace the whole previously saved data in database with the new ones
            else, just save the new ones

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
        # TODO
        pass

    @task
    def discord_extract_raw_members(
        platform_info: dict[str, str | datetime | bool]
    ) -> dict[str, str | list[dict] | bool]:
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
            if recompute was false, then will fetch from the previously saved data date
            if recompute True, then will fetch all platform's members data

        Returns
        --------
        platform_raw_data : dict[str, str | list[dict] | bool]
            the raw data for a platform
            example data
            ```
            {
                platform_id: str,
                raw_data : list[dict],
                recompute : bool,
            }
            ```
        """
        # TODO
        pass

    @task
    def discord_transform_members(
        platform_raw_data: dict[str, str | list[dict] | bool]
    ) -> dict[str, str | list[dict] | bool]:
        """
        transform raw data given for a platform to a general data structure

        Parameters
        ------------
        platform_raw_data : dict[str, str | list[str] | bool]
            the raw members data for a platform
            example data
            ```
            {
                platform_id: str,
                raw_data : list[dict],
                recompute : bool,
            }
            ```

        Returns
        ----------
        transformed_data : dict[str, str | list[dict] | bool]
            the platform's data transformed
            ```
            {
                platform_id: str,
                transformed_data : list[dict],
                recompute : bool,
            }
            ```
        """

    @task
    def discord_load_transformed_members(
        platform_transformed_data: dict[str, str | list[dict] | bool]
    ) -> None:
        """
        load transformed data within database

        Parameters
        ------------
        platform_raw_data : dict[str, str | list[str] | bool]
            the raw data for a platform
            example data
            ```
            {
                platform_id: str,
                raw_data : list[dict],
                recompute : bool,
            }
            ```
            if recompute is True, then replace the whole previously saved data in database with the new ones
            else, just save the new ones (replace the users with duplicate id)
        """
        # TODO
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
        # TODO
        pass

    platform_modules = fetch_discord_platforms()
    raw_memberactivities = discord_extract_raw_data.expand(
        platform_info=platform_modules
    )
    transformed_rawmemberactivities = discord_transform_raw_data.expand(
        platform_raw_data=raw_memberactivities
    )
    discord_processed_data = discord_load_transformed_data(
        platform_transformed_data=transformed_rawmemberactivities
    )

    raw_members = discord_extract_raw_members.expand(platform_info=platform_modules)
    transformed_members = discord_transform_members(platform_raw_data=raw_members)
    member_load_task = discord_load_transformed_members(
        platform_transformed_data=transformed_members
    )

    member_load_task >> analyze_discord(platform_processed=discord_processed_data)
