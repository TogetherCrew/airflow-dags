from bson import ObjectId

from datetime import datetime, timezone
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TelegramPlatform:
    def __init__(self, chat_id: str, chat_name: str) -> None:
        """
        Parameters
        -----------
        chat_id : str
            check if there's any platform exists
        chat_name : str
            the chat name to create later (if not already exists)
        """
        self._client = MongoSingleton.get_instance().get_client()
        self.chat_id = chat_id
        self.chat_name = chat_name

        self.database = "Core"
        self.collection = "platforms"

    def check_platform_existence(self) -> ObjectId | None:
        """
        check if there's any platform exist for a chat_id

        Returns
        --------
        community_id : ObjectId | None
            the community id if available
            else will be None
        """
        document = self._client[self.database][self.collection].find_one(
            {"metadata.id": self.chat_id},
            {
                "community": 1,
            },
        )

        return document["community"] if document else None

    def create_platform(self) -> ObjectId:
        """
        create a platform for the chat_id having the community id

        Returns
        ---------
        community_id : ObjectId
            the community ID that was assigned to a platform
        """
        community_id = ObjectId()
        self._client[self.database][self.collection].insert_one(
            {
                "metadata": {
                    "id": self.chat_id,
                    "name": self.chat_name,
                },
                "community": community_id,
                "disconnectedAt": None,
                "createdAt": datetime.now().replace(tzinfo=timezone.utc),
                "updatedAt": datetime.now().replace(tzinfo=timezone.utc),
            }
        )
        return community_id
