import copy
import logging

from violation_detection_helpers.utils import Classifier

class TransformPlatformRawData:
    def __init__(self) -> None:
        self.classifier = Classifier()

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
        labeled_data = []
        for record in raw_data:
            try:
                data = copy.deepcopy(record)

                text = record["text"]
                label = self.classifier.classify(text)

                data.setdefault("metadata", {})
                data["metadata"]["vdLabel"] = label

                labeled_data.append(data)
            except Exception as exp:
                logging.error(f"Exception raised while classifying document. exp: {exp}")
        
        return labeled_data

