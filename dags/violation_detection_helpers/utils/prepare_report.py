import logging
from typing import Any


class PrepareReport:
    def prepare(self, transformed_documents: list[dict[str, Any]]) -> str | None:
        """
        prepare a report if detected violation using the raw_messages

        Parameters
        -----------
        transformed_documents : list[dict[str, Any]]
            a list of transformed documents with the label `vdLabel` under their metadata
            also documents should have `link` under their metadata field too

        Returns
        --------
        report : str | None
            the report we want to send users
            if no documents given, then would be None meaning not to send report
        """
        reports: list[str] = []
        for document in transformed_documents:
            label = document["metadata"].get("vdLabel")
            link = document["metadata"].get("link")
            id = document["_id"]

            if label and link:
                # report for single document
                # if there was a specific label
                if label != "None":
                    doc_report = f"Document link: {link} | Label: {label}"
                    reports.append(doc_report)
            elif label:
                logging.error(
                    f"Document with _id: {id} doesn't have "
                    "the `metadata.link` field!"
                )
            elif link:
                logging.error(
                    f"Document with _id: {id} doesn't have "
                    "the `metadata.vdLabel` field!"
                )
            else:
                logging.error(
                    f"Document with _id: {id} doesn't have the fields"
                    " `metadata.vdLabel` and `metadata.link`!"
                )

        report: str | None
        if len(reports) != 0:
            report = (
                "Here's a list of messages with detected violation\n\n"
                + "\n".join(list(set(reports)))
            )
        else:
            logging.warning(
                "No reports made since no document was "
                "either given or had the `metadata.vdLabel` and `metadata.link` fields!"
            )
            report = None

        return report
