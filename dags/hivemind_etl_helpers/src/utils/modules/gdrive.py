import logging
from datetime import datetime

from tc_hivemind_backend.db.modules_base import ModulesBase


class ModulesGDrive(ModulesBase):
    def __init__(self) -> None:
        self.platform_name = "google"
        super().__init__()

    def get_learning_platforms(self):
        """
        get google-drive learning platforms with their forum endpoint

        Returns
        ---------
        platforms_data : list[dict[str, str | datetime]]
            a list of google-drive data information

            example data output:
            ```
            [{
                "platform_id": "platform1",
                "community_id": "community1",
                "drive_ids": ["1111", "2222"],
                "folder_ids": ["132", "45232"],
                "file_ids": ["9999", "888"],
                "refresh_token": "3ui2he09w",
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
                    modules_options = platform["metadata"]
                    refresh_token = self.get_token(
                        platform_id=platform_id, token_type="google_refresh"
                    )
                    platforms_data.append(
                        {
                            "platform_id": str(platform_id),
                            "community_id": str(community),
                            "drive_ids": modules_options.get("driveIds", []),
                            "folder_ids": modules_options.get("folderIds", []),
                            "file_ids": modules_options.get("fileIds", []),
                            "refresh_token": refresh_token,
                        }
                    )
                except Exception as exp:
                    logging.error(
                        "Exception while fetching mediaWiki modules "
                        f"for platform: {platform_id} | exception: {exp}"
                    )

        return platforms_data
