import logging
from datetime import datetime, timezone

from bson import ObjectId
from tc_hivemind_backend.db.mongo import MongoSingleton


class TelegramModules:
    def __init__(self, community_id: str, platform_id: str) -> None:
        """
        Parameters
        -----------
        community_id : str
            the community id related to telegram platform
        platform_id : str
            The platform id related to telegram
        """
        self._client = MongoSingleton.get_instance().get_client()
        self.platform_id = platform_id
        self.community_id = community_id

        self.database = "Core"
        self.collection = "modules"

    def create(self):
        """
        create a module if not exists for community_id
        else, add a platform into the module if not exist and else do nothing
        """
        exists = self._check_module_existence()
        if not exists:
            logging.info(
                f"Module doesn't exist for community: {self.community_id}. Creating one."
            )
            self._create_module()
        else:
            logging.info(f"Module already exists for community: {self.community_id}")
            platform_exists = self._check_platform_existence()
            if not platform_exists:
                logging.info(
                    "Adding platform to the already"
                    f" existing community with id: {self.community_id}!"
                )
                self._add_platform_to_community()
            else:
                logging.info("Platform was already added to modules!")

    def _check_module_existence(self) -> bool:
        """
        Check if there's any module that exists for the community_id

        Returns
        --------
        existence : bool
            True, if a community module is already set
            False, if there's no module related to the community
        """
        document = self._client[self.database][self.collection].find_one(
            {"name": "hivemind", "community": ObjectId(self.community_id)},
            {
                "_id": 1,
            },
        )
        return bool(document)

    def _check_platform_existence(self) -> bool:
        """
        check if the platform exist in a module holding the community id
        """
        document = self._client[self.database][self.collection].find_one(
            {
                "name": "hivemind",
                "community": ObjectId(self.community_id),
                "options.platforms.platform": ObjectId(self.platform_id),
            },
            {
                "_id": 1,
            },
        )
        return bool(document)

    def _add_platform_to_community(self) -> bool:
        """
        Having the community_id modules insert the platform into it
        """
        result = self._client[self.database][self.collection].update_one(
            {
                "community": ObjectId(self.community_id),
                "name": "hivemind",
            },
            {
                "$push": {
                    "options.platforms": {
                        "platform": ObjectId(self.platform_id),
                        "name": "telegram",
                        "_id": ObjectId(),
                    }
                },
                "$set": {"updatedAt": datetime.now(timezone.utc)},
                "$inc": {"__v": 1},
            },
        )
        return result.modified_count > 0

    def _create_module(self) -> None:
        """
        create a module for the community holding platform
        """
        self._client[self.database][self.collection].insert_one(
            {
                "name": "hivemind",
                "community": ObjectId(self.community_id),
                "options": {
                    "platforms": [
                        {
                            "platform": ObjectId(self.platform_id),
                            "name": "telegram",
                            "_id": ObjectId(),
                        }
                    ]
                },
                "createdAt": datetime.now().replace(tzinfo=timezone.utc),
                "updatedAt": datetime.now().replace(tzinfo=timezone.utc),
            }
        )
