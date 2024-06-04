from dags.analyzer_helper.discord.extract_raw_member_base import ExtractRawMembersBase
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class DiscordExtractRawMembers(ExtractRawMembersBase):
    def __init__(self, platform_id: str, guild_id: str):
        """
        Initialize the class for a specific platform
        """
        self.platform_id = platform_id
        self.guild_id = guild_id
        self.client = MongoSingleton.get_instance().client
        self.db = self.client[guild_id]
        self.collection = self.db['guildmembers']

    def extract(self, recompute: bool = False) -> list:
        """
        Extract members data
        if recompute = True, then extract the whole members
        else, start extracting from latest saved member's `joined_at` date

        Note: if the user id was duplicate, then replace.
        """
        if recompute:
            return list(self.collection.find({}))
        else:
            latest_member = self.collection.find().sort("joinedAt", -1).limit(1)
            if latest_member.count() > 0:
                latest_joined_at = latest_member[0]['joinedAt']
                return list(self.collection.find({"joinedAt": {"$gte": latest_joined_at}}))
            else:
                return list(self.collection.find({}))
