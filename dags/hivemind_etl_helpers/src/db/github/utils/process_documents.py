from llama_index import Document
from hivemind_etl_helpers.src.utils.check_documents import check_documents


class ProcessGitHubDocuments:
    def __init__(self, community_id: str, table_name: str = "github") -> None:
        """
        A class for checking if the documents are updated to their latest
        version on database

        Parameters
        -----------
        community_id : str
            the community that the data relates to
            databalse name will be created based on this
        table_name : str
            the table to access github data
            default is `github`
        """
        self.community_id = community_id
        self.table_name = table_name

    def process_issues(
        self, documents: list[Document]
    ) -> tuple[list[Document], list[str]]:
        """
        process issue documents to check if the updatest version is available on db or not

        Parameters
        ------------
        documents: list[llama_index.Document]
            a list of documents transformed from github issues

        Returns
        --------
        documents_to_save : list[llama_index.Document]
            the list of documents to do the embedding and save within database
        doc_file_ids_to_delete : list[str]
            a list of document file ids to delete since they need to be updated
        """
        documents_to_save, doc_file_ids_to_delete = self._check_documents(
            documents, identifier="id", date_field="updated_at"
        )
        return documents_to_save, doc_file_ids_to_delete

    def process_pull_requests(
        self, documents: list[Document]
    ) -> tuple[list[Document], list[str]]:
        """
        process pull request documents to check if the updatest version
        is available on db or not

        Parameters
        ------------
        documents: list[llama_index.Document]
            a list of documents transformed from github pull requests

        Returns
        --------
        documents_to_save : list[llama_index.Document]
            the list of documents to do the embedding and save within database
        doc_file_ids_to_delete : list[str]
            a list of document file ids to delete since they need to be updated
        """
        # the closed_at field was updated
        closed_documents_to_save, closed_doc_file_ids_to_delete = self._check_documents(
            documents, identifier="id", date_field="closed_at"
        )
        # the merged_at field was updated
        merged_documents_to_save, merged_doc_file_ids_to_delete = self._check_documents(
            documents, identifier="id", date_field="merged_at"
        )

        documents_to_save: list[Document] = []
        documents_to_save.extend(closed_documents_to_save)
        documents_to_save.extend(merged_documents_to_save)

        doc_file_ids_to_delete: list[str] = []
        doc_file_ids_to_delete.extend(closed_doc_file_ids_to_delete)
        doc_file_ids_to_delete.extend(merged_doc_file_ids_to_delete)

        return documents_to_save, doc_file_ids_to_delete

    def process_comments(
        self, documents: list[Document]
    ) -> tuple[list[Document], list[str]]:
        """
        process comment documents to check if the updatest version is
        available on db or not

        Parameters
        ------------
        documents: list[llama_index.Document]
            a list of documents transformed from github comments

        Returns
        --------
        documents_to_save : list[llama_index.Document]
            the list of documents to do the embedding and save within database
        doc_file_ids_to_delete : list[str]
            a list of document file ids to delete since they need to be updated
        """
        documents_to_save, doc_file_ids_to_delete = self._check_documents(
            documents, identifier="id", date_field="updated_at"
        )
        return documents_to_save, doc_file_ids_to_delete

    def _check_documents(self, docs: list[Document], identifier: int, date_field: str):
        """
        check documents and return the documents to save and document file ids to delete
        """
        documents_to_save, doc_file_ids_to_delete = check_documents(
            documents=docs,
            community_id=self.community_id,
            identifier=identifier,
            # identifier_type="::int",
            date_field=date_field,
            table_name=self.table_name,
        )
        return documents_to_save, doc_file_ids_to_delete
