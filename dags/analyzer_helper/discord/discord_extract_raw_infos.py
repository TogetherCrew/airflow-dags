from datetime import datetime
from analyzer_helper.discord.extract_raw_info_base import ExtractRawInfosBase
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class DiscordExtractRawInfos(ExtractRawInfosBase):
    def __init__(self, platform_id: str):
        super().__init__(platform_id)
        self.client = MongoSingleton.get_instance().client
        self.db = self.client[self.get_platform_id()]
        self.collection = self.db['rawmemberactivities']

    def extract(self, period: datetime, recompute: bool = False) -> list:
        if recompute:
            return list(self.collection.find({}))
        else:
            return list(self.collection.find({'date': {'$gte': period}}))
