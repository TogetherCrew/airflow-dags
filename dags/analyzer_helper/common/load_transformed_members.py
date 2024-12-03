import logging
import pymongo

from analyzer_helper.common.base.load_transformed_members_base import (
    LoadTransformedMembersBase,
)
from tc_hivemind_backend.db.mongo import MongoSingleton


class LoadTransformedMembers(LoadTransformedMembersBase):
    def __init__(self, platform_id: str):
        super().__init__(platform_id)
        # self._platform_id = platform_id
        self.client = MongoSingleton.get_instance().client
        self.db = self.client[self.get_platform_id()]
        # self.db = self.client[self._platform_id]
        self.collection = self.db["rawmembers"]

    def load(self, processed_data: list[dict], recompute: bool = False):
        if recompute:
            logging.info("Recompute is true, deleting all the previous data!")
            self.collection.delete_many({})
            self.collection.insert_many(processed_data)
            return

        # Perform bulk updates
        bulk_operations = []
        for document in processed_data:
            # Using document's id as the unique identifier
            filter_criteria = {"id": document["id"]}
            bulk_operations.append(
                pymongo.UpdateOne(filter_criteria, {"$set": document}, upsert=True)
            )

        if bulk_operations:
            try:
                # Execute bulk write operations
                result = self.collection.bulk_write(bulk_operations, ordered=False)
                logging.info(f"Matched {result.matched_count} documents")
                logging.info(f"Modified {result.modified_count} documents")
                logging.info(f"Upserted {result.upserted_count} documents")
            except Exception as e:
                logging.error(f"Error during bulk write operation: {str(e)}")
                raise
