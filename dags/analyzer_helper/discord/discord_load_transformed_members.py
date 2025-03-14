import logging

from analyzer_helper.discord.load_transformed_members_base import (
    LoadTransformedMembersBase,
)
from tc_hivemind_backend.db.mongo import MongoSingleton


class DiscordLoadTransformedMembers(LoadTransformedMembersBase):
    def __init__(self, platform_id: str):
        # super().__init__(platform_id)
        self._platform_id = platform_id
        self.client = MongoSingleton.get_instance().client
        # self.db = self.client[self.get_platform_id()]
        self.db = self.client[self._platform_id]
        self.collection = self.db["rawmembers"]

    def load(self, processed_data: list[dict], recompute: bool = False):
        if recompute:
            logging.info("Recompute is true, deleting all the previous data!")
            self.collection.delete_many({})
        self.collection.insert_many(processed_data)
