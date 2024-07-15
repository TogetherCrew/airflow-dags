from bson import ObjectId
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class FetchDiscordPlatforms:
    """
    A class to fetch Discord platform data from a MongoDB collection.

    Attributes:
        client (MongoClient): The MongoDB client.
        db (Database): The MongoDB database.
        collection (Collection): The MongoDB collection.
    """

    def __init__(self, db_name="Core", collection="platforms"):
        """
        Initializes the FetchDiscordPlatforms class.

        Args:
            db_name (str): The name of the database. Defaults to 'Core'.
            collection (str): The name of the collection. Defaults to 'platforms'.
        """
        self.client = MongoSingleton.get_instance().client
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
        """
        query = {
            "disconnectedAt": None,
            "name": "discord",
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
                "recompute": False,
                "guild_id": doc.get("metadata", {}).get("id", None),
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
            "name": "discord",
        }
        projection = {
            "_id": 1,
            "metadata.period": 1,
            "metadata.action": 1,
            "metadata.window": 1,
            "metadata.selectedChannels": 1,
            "metadata.id": 1,
        }

        platform = self.collection.find_one(query, projection)

        if platform is not None:
            platform_data = {
                "platform_id": str(platform["_id"]),
                "metadata": {
                    "period": platform.get("metadata", {}).get("period", None),
                    "action": platform.get("metadata", {}).get("action", None),
                    "window": platform.get("metadata", {}).get("window", None),
                    "selectedChannels": platform.get("metadata", {}).get(
                        "selectedChannels", None
                    ),
                    "id": platform.get("metadata", {}).get("id", None),
                },
                "recompute": False,
            }
            return platform_data
        else:
            raise ValueError(
                f"No platform given platform_id: {platform_id} is available!"
            )


    # TODO: Decide if we'd like to merge `fetch_all` and `fetch_analyzer_parameters`
    # def fetch_for_analyzer(self, platform_id: str):
    #     """
    #     Fetches the specified Discord platform from the MongoDB collection with additional fields.

    #     Parameters:
    #         platform_id (str): The platform ID to fetch.

    #     Returns:
    #         dict: A dictionary containing the platform data with the following fields:
    #             - platform_id: The platform ID (_id from MongoDB).
    #             - metadata: A dictionary containing period, action, window, selectedChannels, and id.
    #             - recompute: A boolean set to False.
    #     """
    #     query = {
    #         "_id": platform_id,
    #         "disconnectedAt": None,
    #         "platform": "discord",
    #     }
    #     projection = {
    #         "_id": 1,
    #         "metadata.period": 1,
    #         "metadata.action": 1,
    #         "metadata.window": 1,
    #         "metadata.selectedChannels": 1,
    #         "metadata.id": 1,
    #     }

    #     doc = self.collection.find_one(query, projection)

    #     if doc:
    #         metadata = {
    #             "period": doc.get("metadata", {}).get("period", None),
    #             "id": doc.get("metadata", {}).get("id", None)
    #         }

    #         if "metadata.action" in projection:
    #             metadata["action"] = doc.get("metadata", {}).get("action", None)

    #         if "metadata.window" in projection:
    #             metadata["window"] = doc.get("metadata", {}).get("window", None)

    #         platform_data = {
    #             "platform_id": str(doc["_id"]),
    #             "metadata": metadata,
    #             "recompute": False,
    #         }

    #         return platform_data
