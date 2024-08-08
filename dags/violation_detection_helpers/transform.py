import copy
import logging

from pymongo.cursor import Cursor
from violation_detection_helpers.utils import Classifier


class TransformPlatformRawData:
    def __init__(self) -> None:
        self.classifier = Classifier()

    def transform(
        self,
        raw_data: Cursor,
    ) -> list[dict]:
        """
        transform a list of platform's `rawmemberactivities` by labeling them

        Parameters
        -------------
        raw_data : Cursor
            the data cursor to be transformed
            (using cursor for more efficiency of database)
            the transformation here is to label the violation for texts

        Returns
        ----------
        labeled_data : list[dict]
            the same data but with a label for violation detection
        """
        labeled_data = []

        # caching label per source_id
        # since we might have multiple document with same text
        cached_label: dict[str, str] = {}

        for record in raw_data:
            try:
                data = copy.deepcopy(record)

                text = record["text"]

                # if not labeled before
                if data["source_id"] not in cached_label:
                    cached_label[data["source_id"]] = self.classifier.classify(text)

                label = cached_label[data["source_id"]]

                data.setdefault("metadata", {})
                data["metadata"]["vdLabel"] = label

                labeled_data.append(data)
            except Exception as exp:
                logging.error(
                    f"Exception raised while classifying document. exp: {exp}"
                )

        return labeled_data
