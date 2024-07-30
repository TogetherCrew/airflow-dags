import logging

from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class ViolationDetectionModules:
    def __init__(self) -> None:
        self.client = MongoSingleton.get_instance().get_client()

    def retrieve_platforms(self) -> list[dict]:
        """
        retrieve platforms to be processed for Violation Detection (VD)

        Returns
        ----------
        vd_platforms : list[dict]
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
        cursor = self.client["Core"]["modules"].find(
            {
                "name": "violationDetection",
            }
        )

        vd_platforms = []
        for module in cursor:
            community_id = str(module["community"])
            platforms = module.get("options", {}).get("platforms", [])
            for platform in platforms:
                platform_id = str(platform["platform"])
                metadata = platform["metadata"]
                resources = metadata["selectedResources"]
                discord_users = metadata["selectedDiscordUsers"]
                from_date = metadata["fromDate"]
                to_date = metadata["toDate"]

                vd_platforms.append(
                    {
                        "community": community_id,
                        "platform_id": platform_id,
                        "resources": resources,
                        "selected_discord_users": discord_users,
                        "from_date": from_date,
                        "to_date": to_date,
                    }
                )

        return vd_platforms
