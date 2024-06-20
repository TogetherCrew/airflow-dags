from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class UserBotChecker:
    """
    A class to check if a user is a bot in a specific platform.

    Attributes:
        platform_id (str): The ID of the platform.
        client (MongoClient): The MongoDB client instance.
        db (Database): The database instance for the platform.
        guildmembers_collection (Collection): The collection of guild members.
    """

    def __init__(self, platform_id):
        """
        Initializes the UserBotChecker with the given platform ID.

        Args:
            platform_id (str): The ID of the platform.
        """
        self.client = MongoSingleton.get_instance().client
        self.platform_id = platform_id
        self.db = self.client[self.platform_id]
        self.guildmembers_collection = self.db["guildmembers"]

    def is_user_bot(self, author_id):
        """
        Checks if a user is a bot by querying the guildmembers collection.

        Args:
            author_id (str): The ID of the author to check.

        Returns:
            bool: True if the user is a bot, False otherwise.
        """
        result = self.guildmembers_collection.find_one(
            {"discordId": author_id}, {"isBot": 1, "_id": 0}
        )
        print(f"User ID: {author_id}, isBot: {result.get('isBot', False) if result else False}")

        return result.get("isBot", False) if result else False
