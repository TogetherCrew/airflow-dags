from analyzer_helper.discord.extract_raw_member_base import ExtractRawMembersBase
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class DiscordExtractRawMembers(ExtractRawMembersBase):
    def __init__(self, guild_id: str, platform_id: str):
        """
        Initialize the class for a specific guild and platform
        """
        self.guild_id = guild_id
        self.platform_id = platform_id
        self.client = MongoSingleton.get_instance().client
        self.guild_db = self.client[guild_id]
        self.platform_db = self.client[platform_id]
        self.guild_collection = self.guild_db["guildmembers"]
        self.rawmembers_collection = self.platform_db["rawmembers"]

    def extract(self, recompute: bool = False) -> list:
        """
        Extract members data
        if recompute = True, then extract the whole members
        else, start extracting from the latest saved member's `joined_at` date

        Note: if the user id was duplicate, then replace.
        """
        members = []
        if recompute:
            members = list(self.guild_collection.find({}))
        else:
            # Fetch the latest joined date from rawmembers collection
            latest_rawmember = self.rawmembers_collection.find_one(
                sort=[("joined_at", -1)]
            )
            latest_joined_at = (
                latest_rawmember["joined_at"] if latest_rawmember else None
            )

            if latest_joined_at:
                members = list(
                    self.guild_collection.find({"joinedAt": {"$gte": latest_joined_at}})
                )
            else:
                members = list(self.guild_collection.find({}))

        return members
