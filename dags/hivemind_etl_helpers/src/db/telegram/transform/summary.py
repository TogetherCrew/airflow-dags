from datetime import date, datetime, timedelta, timezone

from llama_index.core import Document


class TransformSummary:
    def __init__(self) -> None:
        pass

    def transform(self, summaries: dict[date, str]) -> list[Document]:
        """
        transform daily summaries to llama-index documents

        Parameters
        -----------
        summaries : dict[date, str]
            daily summaries

        Returns
        --------
        summary_docs : list[llama_index.core.Document]
            llama-index documents for summaries
        """
        summary_docs: list[Document] = []

        for day, summary in summaries.items():
            # assigning an id so it would be consistent across different runs
            document = Document(
                doc_id="summary_"
                + day.strftime("%Y-%m-%d")
                + "_"
                + (day + timedelta(days=1)).strftime("%Y-%m-%d"),
                text=summary,
                metadata={
                    "date": day.strftime("%Y-%m-%d"),
                },
            )
            summary_docs.append(document)

        return summary_docs
