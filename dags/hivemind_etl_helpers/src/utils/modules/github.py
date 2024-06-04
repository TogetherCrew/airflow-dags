import logging
from datetime import datetime

from .modules_base import ModulesBase


class ModulesGitHub(ModulesBase):
    def __init__(self) -> None:
        self.platform_name = "github"
        super().__init__()

    def get_learning_platforms(self):
        """
        get discourse learning platforms with their forum endpoint

        Returns
        ---------
        platforms_data : list[dict[str, str | datetime]]
            a list of discourse data information

            example data output:
            ```
            [{
                "community_id": "community1",
                "organization_ids": ["1111", "2222"],
                "repo_ids": ["132", "45232"],
                # "from_date": datetime(2024, 1, 1)
                "from_date": None
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

                platform_id = platform["platform"]

                try:
                    account = self.get_platform_metadata(
                        platform_id=platform_id,
                        metadata_name="account",
                    )
                    organization_id = account["id"]
                    modules_options = platform["metadata"]

                    # if github modules was activated
                    if modules_options["activated"] is True:
                        platforms_data.append(
                            {
                                "community_id": str(community),
                                "organization_ids": [organization_id],
                                # "repo_ids": modules_options.get("repoIds", []),
                                # "from_date": modules_options["fromDate"],
                                "from_date": None,
                            }
                        )
                except Exception as exp:
                    logging.error(
                        "Exception while fetching mediaWiki modules "
                        f"for platform: {platform_id} | exception: {exp}"
                    )

        return platforms_data
