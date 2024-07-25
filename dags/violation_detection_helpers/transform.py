from datetime import datetime

from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TransformPlatformRawData:
    def __init__(self) -> None:
        self.client = MongoSingleton.get_instance().get_client()

    def transform(
        self,
        platform_id: str,
        raw_data: list[dict],
    ) -> list[dict]:
        """
        transform a list of platform's `rawmemberactivities` by labeling them

        Parameters
        -------------
        platform_id : str
            the platform to be used
        raw_data : list[dict]
            the extracted data to be transformed
            the transformation here is to label the violation for texts

        Returns
        ----------
        labeled_data : list[dict]
            the same data but with a label for violation detection
        """
        raise NotImplementedError
