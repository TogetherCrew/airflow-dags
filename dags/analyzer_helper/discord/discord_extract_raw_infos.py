import logging
from datetime import datetime, timezone

from analyzer_helper.discord.extract_raw_info_base import ExtractRawInfosBase
from tc_hivemind_backend.db.mongo import MongoSingleton


class DiscordExtractRawInfos(ExtractRawInfosBase):
    def __init__(self, guild_id: str, platform_id: str):
        """
        Initializes the class with a specific guild and platform identifier.

        Parameters
        ----------
        guild_id : str
            The identifier for the guild.
        platform_id : str
            The identifier for the platform.
        """
        super().__init__(guild_id)
        self.client = MongoSingleton.get_instance().client
        self.guild_db = self.client[self.get_guild_id()]
        self.platform_db = self.client[platform_id]
        self.collection = self.guild_db["rawinfos"]
        self.rawmemberactivities_collection = self.platform_db["rawmemberactivities"]

    def extract(self, period: datetime, recompute: bool = False) -> list:
        data = []
        if recompute:
            data = list(self.collection.find({}))
        else:
            latest_activity = self.rawmemberactivities_collection.find_one(
                sort=[("date", -1)]
            )
            latest_activity_date = latest_activity["date"] if latest_activity else None

            if latest_activity_date is not None:
                prefix = "previous data is available! "
                if latest_activity_date.replace(tzinfo=timezone.utc) >= period.replace(
                    tzinfo=timezone.utc
                ):
                    logging.info(f"{prefix}Extracting data from {latest_activity_date}")
                    data = list(
                        self.collection.find(
                            {"createdDate": {"$gt": latest_activity_date}}
                        )
                    )
                else:
                    logging.info(f"{prefix}Extracting data from {period}")
                    data = list(self.collection.find({"createdDate": {"$gte": period}}))
            else:
                logging.info("No previous data available! Extracting all")
                data = list(self.collection.find({}))
        return data
