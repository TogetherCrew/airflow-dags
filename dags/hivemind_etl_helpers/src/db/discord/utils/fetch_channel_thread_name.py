from tc_hivemind_backend.db.mongo import MongoSingleton


class FetchDiscordChannelThreadNames:
    def __init__(self, guild_id: str) -> None:
        self.client = MongoSingleton.get_instance().get_client()
        self.db = self.client[guild_id]
        self.channels_collection = self.db["channels"]
        self.threads_collection = self.db["threads"]

    def fetch_discord_channel_name(self, channel_id: str) -> str | None:
        """
        Fetch the name of a channel from the database.

        Parameters:
        --------------
        channel_id : str
            The ID of the channel to fetch the name of.

        Returns:
        ---------
        str | None
            The name of the channel, or None if the channel is not found.
        """
        query = {"channelId": channel_id}
        channel = self.channels_collection.find_one(query)
        if channel is None:
            return None
        else:
            return channel["name"]

    def fetch_discord_thread_name(self, thread_id: str) -> str | None:
        """
        Fetch the name of a thread from the database.

        Parameters:
        --------------
        thread_id : str
            The ID of the thread to fetch the name of.

        Returns:
        ---------
        str | None
            The name of the thread, or None if the thread is not found.
        """
        query = {"id": thread_id}
        thread = self.threads_collection.find_one(query)
        if thread is None:
            return None
        else:
            return thread["name"]
