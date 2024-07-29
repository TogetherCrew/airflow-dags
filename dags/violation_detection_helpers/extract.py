import logging
from datetime import datetime
from pymongo.cursor import Cursor

from hivemind_etl_helpers.src.utils.mongo import MongoSingleton
from tc_analyzer_lib.schemas.platform_configs.config_base import PlatformConfigBase


class ExtractPlatformRawData:
    def __init__(self, platform_id: str, analyzer_config: PlatformConfigBase) -> None:
        self.client = MongoSingleton.get_instance().get_client()
        self.platform_id = platform_id

        # the resource to query
        self.resource_name = "metadata." + analyzer_config.resource_identifier

    def extract(
        self,
        from_date: datetime,
        to_date: datetime | None,
        resources: list[str],
        recompute: bool = False,
    ) -> Cursor:
        """
        extract a list of platform's `rawmemberactivities` data

        Parameters
        -------------
        platform_id : str
            the platform to be used
        from_date : datetime
            extract data from a specific date
        to_date : datetime | None
            extract data to a specific date
            if `None`, no filtering would be applied to it
        resources : list[str]
            a list of resources to extract data from
        recompute : bool
            if `False`, extract the non-labeled data after the latest ones
            if `True`, extract `rawmemberactivities` data from the given date
            to date
            default is `False` which means to extract from the latest processed one

        Returns
        ---------
        raw_data : Cursor
            a list of raw data to be processed
        """
        date_query: dict
        if not recompute:
            latest_labeled_date = self._find_latest_labeled()

            # all data was processed before
            if to_date and latest_labeled_date and latest_labeled_date >= to_date:
                logging.info(
                    f"All data for platform_id: {self.platform_id} was labeled before!"
                )
                return []

            if latest_labeled_date:
                date_query = {
                    "date": {
                        "$gte": latest_labeled_date if latest_labeled_date > from_date else from_date,
                        "$lte": to_date if to_date else datetime.now(),
                    }
                }
            else:
                date_query = {
                    "date": {"$gte": from_date, "$lte": to_date},
                }

        else:
            date_query = {
                "date": {"$gte": from_date, "$lte": to_date},
            }

        cursor = self.client[self.platform_id]["rawmemberactivities"].find(
            {
                **date_query,
                "source_id": {"$in": resources},
            }
        )
        return cursor


    def _find_latest_labeled(self, label_field: str = "vdLabel") -> datetime | None:
        """
        find the latest labeled document date

        Parameters
        -----------
        label_field : str
            the field that label was saved
 
        Returns
        --------
        latest_labeled_date : datetime | None
            the latest document which was labeled with vdLabel    
        """
        cursor = self.client[self.platform_id]["rawmemberactivities"].find(
            {label_field: {"$ne": None}}, {"date": 1},
        ).sort("date", -1).limit(1)
        document = list(cursor)

        latest_labeled_date: datetime | None
        if document == []:
            latest_labeled_date = None
        else:
            latest_labeled_date = document["date"]

        return latest_labeled_date
