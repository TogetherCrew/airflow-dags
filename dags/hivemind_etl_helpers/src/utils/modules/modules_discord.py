from datetime import datetime

from .modules_base import ModulesBase


class ModulesDiscord(ModulesBase):
    def __init__(self) -> None:
        self.platform_name = "discord"
        super().__init__()

    def get_communities(self) -> list[str]:
        """
        get all community ids for discord

        Returns
        --------
        community_ids : list[str]
            all the discord community ids associated with modules
        """
        community_ids = self.get_platform_community_ids(
            platform_name=self.platform_name
        )
        return community_ids

    def get_learning_platforms(self):
        """
        get discord learning platforms with their forum endpoint

        Returns
        ---------
        platforms_data : list[dict[str, str | datetime]]
            a list of discord data information

            example data output:
            ```
            [{
                "platform_id": "platform_id1",
                "community_id": "community1",
                "selected_channels": ["123", "124"],
                "from_date": datetime(2024, 1, 1)
            }]
            ```
        """
        modules = self.query(platform=self.platform_name, projection={"name": 0})
        platforms_data: list[dict[str, str | datetime]] = []

        # for each community module
        for module in modules:
            community = module["community"]

            # each platform of the community
            for platform in module["options"]["platforms"]:

                if platform["name"] != self.platform_name:
                    continue

                # learning is for doing ETL on data
                if "learning" in platform["metadata"]:
                    learning_config = platform["metadata"]["learning"]
                    platform_id = str(platform["platform"])

                    platforms_data.append(
                        {
                            "platform_id": platform_id,
                            "community_id": str(community),
                            "selected_channels": learning_config["selectedChannels"],
                            "from_date": learning_config["fromDate"],
                        }
                    )

        return platforms_data
