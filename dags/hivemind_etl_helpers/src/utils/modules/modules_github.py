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
                "organization_id": "12345",
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

                    platforms_data.append(
                        {
                            "community_id": str(community),
                            "organization_id": learning_config["organizationId"],
                            "from_date": learning_config["fromDate"],
                        }
                    )

        return platforms_data
