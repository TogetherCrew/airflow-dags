from hivemind_etl_helpers.src.utils.mongo import MongoSingleton
from pymongo import UpdateOne


class LoadPlatformLabeledData:
    def __init__(self) -> None:
        self.client = MongoSingleton.get_instance().get_client()

    def load(
        self,
        platform_id: str,
        transformed_data: list[dict],
    ) -> None:
        """
        update `rawmemberactivities` by with their label

        Parameters
        -------------
        platform_id : str
            the platform to be used
        transformed_data : list[dict]
            the extracted data to be transformed
            the transformation here is to label the violation for texts
        """
        updates = self._prepare_updates(transformed_data)
        self.client[platform_id]["rawmemberactivities"].bulk_write(updates)

    def _prepare_updates(self, transformed_data: list[dict]) -> list[UpdateOne]:
        """
        prepare a list of `UpdateOne` operations to do on database

        Parameters
        -----------
        transformed_data : list[dict]
            the data transformed by labels added
        """
        updates = []
        for document in transformed_data:
            updates.append(
                UpdateOne(
                    filter={"_id": document["_id"]},
                    update={"$set": {**document}},
                    upsert=True,
                )
            )

        return updates
