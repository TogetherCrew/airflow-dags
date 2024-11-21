import logging

from tc_hivemind_backend.db.mongo import MongoSingleton


class ViolationDetectionModules:
    def __init__(self) -> None:
        self.client = MongoSingleton.get_instance().get_client()

    def retrieve_platforms(self, platform_name: str | None = None) -> list[dict]:
        """
        retrieve platforms to be processed for Violation Detection (VD)

        Parameters
        -----------
        platform_name : str | None
            if `None` the would fetch all platforms

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
                selected_emails: list[str],
                from_date: datetime,
                to_date: datetime,
            }
            ```
        """
        logging.info("Extracting all violation_detection modules!")

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

                if platform_name is not None and platform_name != platform["name"]:
                    continue

                try:
                    metadata = platform["metadata"]
                    resources = metadata["selectedResources"]
                    discord_users = metadata["selectedEmails"]
                    from_date = metadata["fromDate"]
                    to_date = metadata["toDate"]

                    vd_platforms.append(
                        {
                            "community": community_id,
                            "platform_id": platform_id,
                            "resources": resources,
                            "selected_emails": discord_users,
                            "from_date": from_date,
                            "to_date": to_date,
                            "recompute": False,  # for default it's False
                        }
                    )
                except Exception as exp:
                    logging.error(
                        f"Exception raised during extracting platform_id: "
                        f"{platform_id}! Exception: {exp}"
                    )

        return vd_platforms
