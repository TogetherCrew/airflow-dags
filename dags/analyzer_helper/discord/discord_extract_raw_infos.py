from datetime import datetime
from analyzer_helper.discord.extract_raw_info_base import ExtractRawInfosBase
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class DiscordExtractRawInfos(ExtractRawInfosBase):
    def __init__(self, platform_id: str):
        """
        Initializes the class with a specific platform identifier.

        Parameters
        ----------
        platform_id : str
            The identifier for the platform.
        """
        super().__init__(platform_id)
        self.client = MongoSingleton.get_instance().client
        self.db = self.client[self.get_platform_id()]
        self.collection = self.db['rawmemberactivities']

    def extract(self, period: datetime, recompute: bool = False) -> list:
        """
        Extracts raw information data from the 'rawmemberactivities' collection.

        Parameters
        ----------
        period : datetime
            The starting date from which data should be extracted.
        recompute : bool, optional
            If True, extracts all data from the collection. If False, extracts data 
            starting from the latest saved record's 'createdDate'.

        Returns
        -------
        list
            A list of documents from the 'rawmemberactivities' collection.
        """
        if recompute:
            data = list(self.collection.find({}))
        else:
            # Fetch the latest date
            latest_record = self.collection.find_one(sort=[("createdDate", -1)])
            latest_date = latest_record["createdDate"] if latest_record else None

            if latest_date and latest_date > period:
                data = list(self.collection.find({'createdDate': {'$gt': latest_date}}))
            else:
                data = list(self.collection.find({'createdDate': {'$gte': period}}))

        return data
