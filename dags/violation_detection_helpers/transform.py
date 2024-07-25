from datetime import datetime

from openai import OpenAI


class TransformPlatformRawData:
    def __init__(self) -> None:
        self.open_ai = OpenAI()

    def transform(
        self,
        raw_data: list[dict],
    ) -> list[dict]:
        """
        transform a list of platform's `rawmemberactivities` by labeling them

        Parameters
        -------------
        raw_data : list[dict]
            the extracted data to be transformed
            the transformation here is to label the violation for texts

        Returns
        ----------
        labeled_data : list[dict]
            the same data but with a label for violation detection
        """
        raise NotImplementedError
