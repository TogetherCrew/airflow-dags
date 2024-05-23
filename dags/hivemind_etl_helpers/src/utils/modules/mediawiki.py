from .modules_base import ModulesBase


class ModulesMediaWiki(ModulesBase):
    def __init__(self) -> None:
        self.platform_name = "mediaWiki"
        super().__init__()

    def get_learning_platforms(
        self,
    ) -> list[dict[str, str | list[str]]]:
        """
        Get all the MediaWiki communities with their page titles.

        Returns
        ---------
        community_orgs : list[dict[str, str | list[str]]] = []
            a list of MediaWiki data information

            example data output:
            ```
            [{
                "community_id": "6579c364f1120850414e0dc5",
                "page_titles": ["Main_Page", "Default_Page"],
                "base_url": "some_api_url",
            }]
            ```
        """
        modules = self.query(platform=self.platform_name, projection={"name": 0})
        communities_data: list[dict[str, str | list[str]]] = []

        for module in modules:
            community = module["community"]

            # each platform of the community
            for platform in module["options"]["platforms"]:
                if platform["name"] != self.platform_name:
                    continue

                base_url = self.get_platform_metadata(
                    platform_id=platform["platform"],
                    metadata_name="baseURL",
                )
                modules_options = platform["metadata"]
                communities_data.append(
                    {
                        "community_id": str(community),
                        "page_titles": modules_options.get("pageIds", []),
                        "base_url": base_url,
                    }
                )

        return communities_data
