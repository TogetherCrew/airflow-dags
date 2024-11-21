import logging

from analyzer_helper.common.base.load_transformed_data_base import (
    LoadTransformedDataBase,
)
from tc_hivemind_backend.db.mongo import MongoSingleton


class LoadTransformedData(LoadTransformedDataBase):
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
