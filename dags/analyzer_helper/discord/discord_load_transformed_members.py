from analyzer_helper.discord.load_transformed_members_base import (
    LoadTransformedMembersBase,
)
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class DiscordLoadTransformedMembers(LoadTransformedMembersBase):
    def __init__(self, platform_id: str):
        super().__init__(platform_id)
        self.client = MongoSingleton.get_instance().client
        self.db = self.client[self.get_platform_id()]
        self.collection = self.db["members"]

    def load(self, processed_data: list[dict], recompute: bool = False):
        # Probably too aggressive, we need to define another DAG for it
        if recompute:
            self.collection.delete_many({})
        self.collection.insert_many(processed_data)
