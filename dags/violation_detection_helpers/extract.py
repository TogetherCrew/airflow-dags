import logging
from datetime import datetime

from pymongo.cursor import Cursor
from tc_hivemind_backend.db.mongo import MongoSingleton


class ExtractPlatformRawData:
    def __init__(self, platform_id: str, resource_identifier: str) -> None:
        self.client = MongoSingleton.get_instance().get_client()
        self.platform_id = platform_id

        # the resource to query
        self.resource_name = "metadata." + resource_identifier

    def extract(
        self,
        from_date: datetime,
        to_date: datetime | None,
        resources: list[str],
        recompute: bool = False,
    ) -> tuple[Cursor, bool]:
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
            if empty, process all messages
        recompute : bool
            if `False`, extract the non-labeled data after the latest ones
            if `True`, extract `rawmemberactivities` data from the given date
            to date
            default is `False` which means to extract from the latest processed one

        Returns
        ---------
        raw_data : Cursor
            a list of raw data to be processed
        override_recompute : bool
            in case we wanted to override the `recompute` to be `True`
            normally would happen if no data was labeled
        """
        date_query: dict

        # in case we wanted to override recompute
        # normally would happen if no data was labeled
        override_recompute: bool = False

        if not recompute:
            latest_labeled_date = self._find_latest_labeled()
            lte_query = to_date if to_date else datetime.now()

            # all data was processed before
            if to_date and latest_labeled_date and latest_labeled_date >= to_date:
                logging.info(
                    f"All data for platform_id: {self.platform_id} was labeled before!"
                )
                return (
                    self.client[self.platform_id]["rawmemberactivities"].find(
                        {"_id": None}
                    ),
                    override_recompute,
                )

            if latest_labeled_date:
                date_query = {
                    "date": {
                        "$gt": (
                            latest_labeled_date
                            if latest_labeled_date > from_date
                            else from_date
                        ),
                        "$lte": lte_query,
                    }
                }
            else:
                override_recompute = True
                date_query = {
                    "date": {
                        "$gte": from_date,
                        "$lte": lte_query,
                    },
                }

        else:
            date_query = {
                "date": {
                    "$gte": from_date,
                    "$lte": lte_query,
                },
            }

        query = {**date_query, "text": {"$ne": None}}

        # Add resource filter only if resources are not empty
        if resources:
            query[self.resource_name] = {"$in": resources}

        cursor = self.client[self.platform_id]["rawmemberactivities"].find(query)

        return cursor, override_recompute

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
        cursor = (
            self.client[self.platform_id]["rawmemberactivities"]
            .find(
                {"metadata." + label_field: {"$ne": None}},
                {"date": 1},
            )
            .sort("date", -1)
            .limit(1)
        )
        document = list(cursor)

        latest_labeled_date: datetime | None = (
            None if document == [] else document[0]["date"]
        )

        return latest_labeled_date
