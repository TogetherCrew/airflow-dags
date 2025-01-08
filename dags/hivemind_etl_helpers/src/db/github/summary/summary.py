from hivemind_etl_helpers.src.db.github.summary.type import SummaryType
from hivemind_etl_helpers.src.utils.summary.summary_base import SummaryBase
from llama_index.core import Document, Settings


class GitHubSummary(SummaryBase):
    def __init__(self, response_synthesizer=None, verbose=False, **kwargs):
        llm = kwargs.get("llm", Settings.llm)
        super().__init__(llm, response_synthesizer, verbose)

        self.postfix = (
            ". Organize the output in one or multiple descriptive "
            "bullet points and include important details"
        )
        self.prefix = (
            "Please make a concise summary based only on the provided text from this"
        )

    def process_prs(self, date: str, documents: list[Document]) -> str:
        """
        summarize the given github documents

        Parameters
        ------------
        date : str
            the date of provided pull requests
        documents : list[Document]
            a list of github pr documents to be summarized

        Returns
        --------
        response : str
            the response of summarizer
        """
        response = self._get_summary(
            summarization_query=self.prefix
            + f"GitHub Pull Requests from date: {date}"
            + self.postfix,
            messages_document=documents,
        )
        return response

    def process_issues(self, date: str, documents: list[Document]) -> str:
        """
        summarize the given github documents

        Parameters
        ------------
        date : str
            the date of provided issues
        documents : list[Document]
            a list of github issues documents to be summarized

        Returns
        --------
        response : str
            the response of summarizer
        """
        response = self._get_summary(
            summarization_query=self.prefix
            + f"GitHub Issues from date: {date}"
            + self.postfix,
            messages_document=documents,
        )
        return response

    def process_comments(self, date: str, documents: list[Document]) -> str:
        """
        summarize the given github documents

        Parameters
        ------------
        date : str
            the date of provided comments
        documents : list[Document]
            a list of github comments documents to be summarized

        Returns
        --------
        response : str
            the response of summarizer
        """
        response = self._get_summary(
            summarization_query=self.prefix
            + f"GitHub Comments from date: {date}"
            + self.postfix,
            messages_document=documents,
        )
        return response

    def process_commits(self, date: str, documents: list[Document]) -> str:
        """
        summarize the given github documents

        Parameters
        ------------
        date : str
            the date of provided commits
        documents : list[Document]
            a list of github commits documents to be summarized


        Returns
        --------
        response : str
            the response of summarizer
        """
        response = self._get_summary(
            summarization_query=self.prefix
            + f"GitHub Commits from date: {date} "
            + self.postfix,
            messages_document=documents,
        )
        return response

    def transform_summary(
        self,
        date: str,
        summary: str,
        type: SummaryType,
    ) -> Document:
        """
        transform the given summary to a llama-index document

        Parameters
        ------------
        date : str
            the date of the provided summary
        summary : str
            summary of an aggregated documents
        type : SummaryType
            the type of summary

        Returns
        ---------
        summary_doc : llama_index.core.Document
            a prepared document of the provided summary
        """
        summary_doc = Document(
            doc_id=f"summary_{date}_{type.value}",
            text=summary,
            metadata={
                "date": date,
                "summary_type": type.value,
            },
        )

        return summary_doc
