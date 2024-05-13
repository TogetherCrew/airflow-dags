from datetime import datetime

from .modules_base import ModulesBase


class ModulesNotion(ModulesBase):
    def __init__(self) -> None:
        self.platform_name = "notion"
        super().__init__()

    def get_all_notion_communities(self) -> list[dict[str, str | list[str] | datetime | dict]]:
        """
        Get all the Notion communities with their database IDs, page IDs, and client config.

        Returns
        ---------
        community_orgs : list[dict[str, str | list[str] | datetime | dict]] = []
            a list of Notion data information

            example data output:
            ```
            [{
            "community_id": "6579c364f1120850414e0dc5",
            "from_date": datetime(2024, 1, 1),
            "database_ids": ["dadd27f1dc1e4fa6b5b9dea76858dabe"],
            "page_ids": ["6a3c20b6861145b29030292120aa03e6"],
            "client_config": {...}
            }]
            ```
        """
        communities_data: list[dict[str, str | list[str] | datetime | dict]] = []
        modules = self.query(platform=self.platform_name, projection={"name": 0})

        for module in modules:
            community = module["community"]

            # each platform of the community
            for platform in module["options"]["platforms"]:
                if platform["name"] != self.platform_name:
                    continue

                modules_options = platform["metadata"]
                communities_data.append(
                    {
                        "community_id": str(community),
                        "from_date": platform.get("fromDate", datetime.now()),
                        "database_ids": modules_options.get("databaseIds", []),
                        "page_ids": modules_options.get("pageIds", []),
                        "client_config": modules_options.get("clientConfig", {})
                    }
                )

        return communities_data
