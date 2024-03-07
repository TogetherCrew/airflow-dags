from hivemind_etl_helpers.src.utils.check_documents import check_documents
from llama_index.core import Document


class PrepareDeletion:
    def __init__(
        self,
        community_id: str,
    ) -> None:
        """
        delete documents wrapper class for GitHub ETL

        Parameters
        ------------
        community_id : str
            the community database to check data within it
        """
        self.community_id = community_id

    def prepare(
        self,
        pr_documents: list[Document],
        issue_documents: list[Document],
        comment_documents: list[Document],
    ) -> tuple[list[Document], str]:
        """
        delete the given documents from db
        Note: documents could be just for the PullRequests, Comments, and Issues

        Parameters
        -------------
        pr_documents : list[llama_index.Document]
            a list of pull request documents to check on database
        issue_documents : list[llama_index.Document]
            a list of issue documents to check on database
        comment_documents : list[llama_index.Document]
            a list of comment documents to check on database

        Returns
        ----------
        documents_to_save : list[llama_index.Document]
            a list of documents to save
        deletion_query : str
            the deletion query used to delete the previous outdated data
        """
        # issue and comment have same properties
        docs_to_save, docs_file_ids_to_delete = self._delete_issue_and_comment_docs(
            issue_documents, comment_documents
        )

        # pull requests are different
        pr_docsuments_to_save, pr_docs_file_ids_to_delete = self._delete_pr_document(
            pr_documents
        )

        documents_to_save = docs_to_save + pr_docsuments_to_save

        deletion_query = self._create_deletion_query(
            docs_file_ids_to_delete + pr_docs_file_ids_to_delete
        )

        return documents_to_save, deletion_query

    def _delete_issue_and_comment_docs(
        self,
        issue_documents: list[Document],
        comment_documents: list[Document],
    ) -> tuple[list[Document], list[str]]:
        documents = issue_documents + comment_documents

        docs_to_save, doc_file_ids_to_delete = self._check_documents(
            documents,
            identifier="id",
            date_field="updated_at",
            identifier_type="::text",
        )
        return docs_to_save, doc_file_ids_to_delete

    def _delete_pr_document(
        self, pr_docs: list[Document]
    ) -> tuple[list[Document], list[str]]:
        docs_merged, docs_merged_pr_ids_to_delete = self._check_documents(
            pr_docs,
            identifier="id",
            date_field="merged_at",
            identifier_type="::integer",
        )

        docs_closed, docs_closed_file_ids_to_delete = self._check_documents(
            pr_docs,
            identifier="id",
            date_field="closed_at",
            identifier_type="::integer",
        )

        doc_file_ids_to_delete = set(
            docs_merged_pr_ids_to_delete + docs_closed_file_ids_to_delete
        )
        documents_to_save = self._get_unique_docs(docs_merged, docs_closed, "id")

        return documents_to_save, list(doc_file_ids_to_delete)

    def _check_documents(
        self,
        documents: list[Document],
        identifier: str,
        date_field: str,
        identifier_type: str,
    ) -> tuple[list[Document], list[str]]:
        """
        a wrapper class for checking previous documents
        """
        docs_to_save, doc_ids_to_delete = check_documents(
            documents,
            community_id=self.community_id,
            identifier=identifier,
            table_name="github",
            date_field=date_field,
            identifier_type=identifier_type,
        )

        return docs_to_save, doc_ids_to_delete

    def _create_deletion_query(
        self,
        doc_ids_to_delete: list[str],
    ) -> str:
        if len(doc_ids_to_delete) == 1:
            deletion_ids = f"({doc_ids_to_delete[0]})"
        else:
            # issues and comments
            deletion_ids = str(tuple([f"{item}" for item in doc_ids_to_delete]))

        deletion_query = f"""
            DELETE FROM data_github
            WHERE (metadata_->>'id')::text IN {deletion_ids};
        """
        return deletion_query

    def _get_unique_docs(
        self, docs_list1: list[Document], docs_list2: list[Document], identifier: str
    ) -> list[Document]:
        """
        get a unique list of documents to save
        the `identifier` is used to compare documents' metadata
        """
        unique_docs = []
        apppended_docs_id = []
        for doc in docs_list1 + docs_list2:
            doc_identifier_value = doc.metadata[identifier]
            if doc_identifier_value not in apppended_docs_id:
                unique_docs.append(doc)
                apppended_docs_id.append(doc_identifier_value)

        return unique_docs
