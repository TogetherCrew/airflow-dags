from bson import ObjectId
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class FetchPlatforms:
    """
    A class to fetch specific platform data from a MongoDB collection.

    Attributes:
        client (MongoClient): The MongoDB client.
        db (Database): The MongoDB database.
        collection (Collection): The MongoDB collection.
    """

    def __init__(
        self, platform_name: str, db_name: str = "Core", collection: str = "platforms"
    ):
        """
        Initializes the FetchDiscordPlatforms class.

        Args:
            platform_name ( str ): The name of the platform we want to fetch ( Discourse, Discord, Telegram, etc )
            db_name (str): The name of the database. Defaults to 'Core'.
            collection (str): The name of the collection. Defaults to 'platforms'.
        """
        self.client = MongoSingleton.get_instance().client
        self.platform_name = platform_name
        self.db = self.client[db_name]
        self.collection = self.db[collection]

    def fetch_all(self):
        """
        Fetches all Discord platforms from the MongoDB collection.

        Returns:
            list: A list of dictionaries, each containing platform data with the following fields:
                - platform_id: The platform ID (_id from MongoDB).
                - metadata: A dictionary containing period, and id.
                - recompute: A boolean set to False.
                - id: forum endpoint
        """
        query = {
            "disconnectedAt": None,
            "name": self.platform_name,
        }
        projection = {
            "_id": 1,
            "metadata.period": 1,
            "metadata.id": 1,
        }

        cursor = self.collection.find(query, projection)
        platforms = []

        for doc in cursor:
            platform_data = {
                "platform_id": str(doc["_id"]),
                "period": doc.get("metadata", {}).get("period", None),
                "id": doc.get("metadata", {}).get("id", None),
                "recompute": False,
            }
            platforms.append(platform_data)

        return platforms

    def fetch_analyzer_parameters(self, platform_id: str) -> dict:
        """
        Fetches the specified Discord platform from the MongoDB collection with additional fields.

        Parameters:
            platform_id (str): The platform ID to fetch.

        Returns:
            dict: A dictionary containing the platform data with the following fields:
                - platform_id: The platform ID (_id from MongoDB).
                - metadata: A dictionary containing period, action, window, selectedChannels, and id.
                - recompute: A boolean set to False.
        """
        query = {
            "_id": ObjectId(platform_id),
            "disconnectedAt": None,
            "name": self.platform_name,
        }
        if self.platform_name == "discord":
            projection = {
                "_id": 1,
                "metadata.period": 1,
                "metadata.action": 1,
                "metadata.window": 1,
                "metadata.selectedChannels": 1,
                "metadata.id": 1,
            }
        else:
            projection = {
                "_id": 1,
                "metadata.period": 1,
                "metadata.action": 1,
                "metadata.window": 1,
                "metadata.resources": 1,
                "metadata.id": 1,
            }

        platform = self.collection.find_one(query, projection)

        if platform:
            if self.platform_name == "discord":
                resources = platform.get("metadata", {}).get("selectedChannels", None)
            else:
                resources = platform.get("metadata", {}).get("resources", None)

            platform_data = {
                "platform_id": str(platform["_id"]),
                "period": platform.get("metadata", {}).get("period", None),
                "action": platform.get("metadata", {}).get("action", None),
                "window": platform.get("metadata", {}).get("window", None),
                "resources": resources,
                "id": platform.get("metadata", {}).get("id", None),
                "recompute": False,
            }

            return platform_data

        else:
            raise ValueError(
                f"No platform given platform_id: {platform_id} is available!"
            )
