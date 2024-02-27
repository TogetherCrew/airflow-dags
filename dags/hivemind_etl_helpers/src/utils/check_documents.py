import logging
from datetime import datetime, timezone

from dateutil import parser
from hivemind_etl_helpers.src.db.gdrive.db_utils import fetch_files_date_field
from llama_index import Document


def check_documents(
    documents: list[Document],
    community_id: str,
    identifier: str,
    date_field: str,
    table_name: str,
    identifier_type: str = "",
    metadata_condition: dict[str, str] | None = None,
) -> tuple[list[Document], list[str]]:
    """
    check documents within the database and if
    it wasn't updated since we saved it under database
    skip that.
    Also return the file id of the data that needed to be deleted
    because they were modified.

    Notes: It is very important to delete the data that ids returned
    and after insert the data. Because in case of exception we would pass the data
    to be deleted and re-inserted.


    Parameters
    ------------
    documents: list[llama_index.Document]
        a list of documents fetched from a platform
    identifier : str
        the identifier of a file or post or any other possible id in any platform
    date_field : str
        the date field representing that the file is updated or not
        in gdrive can be `modified at`, discourse can be `updatedAt` and etc
        the date field values should be in string
    metadata_condition : dict[str, str]
        the metadata of documents with identifier condition as key
        and condition value as the dict value
        Note: For now we support just one condition. so there should be just one
        key and value
    identifier_type : str
        the type conversion needed to be done in getting the
        data having the identifiers.
        default is empty string meaning no conversion to be done in query
        can be `::string`, `::float`, or `::integer`

    Returns
    ---------
    documents_to_save : list[llama_index.Document]
        the list of documents to do the embedding and save within database
    doc_file_ids_to_delete : list[str]
        a list of document file ids to delete since they need to be updated
    """
    documents_to_save: list[Document] = []
    doc_file_ids_to_delete: list[str] = []

    data = process_doc_to_id_date(documents, identifier, date_field)

    # the data within db with a date field as values
    files_db = fetch_files_date_field(
        file_ids=list(data.keys()),
        community_id=community_id,
        identifier=identifier,
        date_field=date_field,
        table_name=table_name,
        metadata_condition=metadata_condition,
        identifier_type=identifier_type,
    )

    for (id, modified_at), doc in zip(data.items(), documents):
        if str(id) in files_db.keys():
            # the modified at of the document in db
            modified_at_db = files_db[str(id)]

            # if the retrieved data had a newer date
            if modified_at is not None and (
                (modified_at_db is None and modified_at is not None)
                or (modified_at_db < modified_at)
            ):
                doc_file_ids_to_delete.append(str(id))
                documents_to_save.append(doc)
            else:
                logging.info(f"Skipping the document with id in metadata: {id}")

        # if the data didn't exist on db or any exceptions was raised
        else:
            doc_file_ids_to_delete.append(str(id))
            documents_to_save.append(doc)

    return documents_to_save, doc_file_ids_to_delete


def process_doc_to_id_date(
    documents: list[Document], identifier: str, date_field: str
) -> dict[str, datetime]:
    """
    process documents into a dictionary of their
    `identifier` as key and `date_field` field as values (extracted from metadata)

    Parameters
    -----------
    documents : list[llama_index.Document]
        the documents to process
    identifier : str
        the identifier of a file or post or any other possible id in any platform
    date_field : str
        the date field representing that the file is updated or not
        in gdrive can be `modified_at`, discourse can be `updatedAt` and etc

    Returns
    --------
    data: dict[str, datetime]
        the processed documents having the file id as keys
        and modified date of the data as datetime object
    """
    # first fetch the documents' modified at
    data: dict[str, datetime] = {}
    for doc in documents:
        file_id = doc.metadata[identifier]
        modified_at = doc.metadata[date_field]
        if modified_at is not None:
            data[file_id] = parser.parse(modified_at).replace(tzinfo=timezone.utc)
        else:
            data[file_id] = None

    return data
