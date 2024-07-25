from datetime import datetime

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
    ) -> list[dict]:
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
        raw_data : list[data]
            a list of raw data to be processed
        """
        raise NotImplementedError
