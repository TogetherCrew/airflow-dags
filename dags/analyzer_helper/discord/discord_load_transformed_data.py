import logging

from analyzer_helper.discord.load_transformed_data_base import LoadTransformedDataBase
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class DiscordLoadTransformedData(LoadTransformedDataBase):
    def __init__(self, platform_id: str):
        super().__init__(platform_id)
        self.client = MongoSingleton.get_instance().client
        self.db = self.client[self.get_platform_id()]
        self.collection = self.db["rawmemberactivities"]

    def load(self, processed_data: list[dict], recompute: bool = False):
        if recompute:
            logging.info("Recompute is true, deleting all the previous data!")
            self.collection.delete_many({})
        self.collection.insert_many(processed_data)
