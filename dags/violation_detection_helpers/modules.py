import logging

from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class ViolationDetectionModules:
    def __init__(self) -> None:
        self.client = MongoSingleton.get_instance().get_client()

    def retrieve_platforms(self) -> list[dict]:
        """
        retrieve platforms to be processed for violation detection

        Returns
        ----------
        platforms : list[dict]
            returns a list of dictionaries representative of platform data
            for violation detection
            each dict data could be as
            ```
            {
                community: str,
                platform_id: str,
                resources: list[str],
                selected_discord_users: list[str],
                from_date: datetime,
                to_date: datetime,
            }
            ```
        """
        raise NotImplementedError
