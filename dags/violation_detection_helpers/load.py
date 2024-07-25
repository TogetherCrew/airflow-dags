from datetime import datetime

from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class LoadPlatformLabeledData:
    def __init__(self) -> None:
        self.client = MongoSingleton.get_instance().get_client()

    def load(
        self,
        platform_id: str,
        transformed_data: list[dict],
    ) -> list[dict]:
        """
        update `rawmemberactivities` by with their label

        Parameters
        -------------
        platform_id : str
            the platform to be used
        transformed_data : list[dict]
            the extracted data to be transformed
            the transformation here is to label the violation for texts

        Returns
        ----------
        labeled_data : list[dict]
            the same data but with a label for violation detection
        """
        raise NotImplementedError
