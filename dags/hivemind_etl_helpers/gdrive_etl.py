import argparse
import logging

from hivemind_etl_helpers.src.db.gdrive.db_utils import setup_db
from hivemind_etl_helpers.src.db.gdrive.retrieve_documents import (
    retrieve_file_documents,
    retrieve_folder_documents,
)
from hivemind_etl_helpers.src.document_node_parser import configure_node_parser
from hivemind_etl_helpers.src.utils.check_documents import check_documents
from hivemind_etl_helpers.src.utils.pg_vector_access import PGVectorAccess
from tc_hivemind_backend.db.utils.model_hyperparams import load_model_hyperparams
from tc_hivemind_backend.embeddings.cohere import CohereEmbedding


def process_gdrive(
    community_id: str, folder_id: str | None = None, file_ids: list[str] | None = None
) -> None:
    """
    process the google drive files
    and save the processed data within postgresql

    Parameters
    -----------
    community_id : str
        the community to save its data
    folder_id : str | None
        the folder id to process its data
        default is None
    file_ids : list[str] | None
        the file ids to process their data
        default is None

    Note: One of `folder_id` or `file_ids` should be given.
    """
    chunk_size, embedding_dim = load_model_hyperparams()
    table_name = "gdrive"
    dbname = f"community_{community_id}"

    if folder_id is None and file_ids is None:
        raise ValueError("At least one of the `folder_id` or `file_ids` must be given!")

    setup_db(community_id=community_id)

    try:
        documents = []
        if folder_id is not None:
            docs = retrieve_folder_documents(folder_id=folder_id)
            documents.extend(docs)
        if file_ids is not None:
            docs = retrieve_file_documents(file_ids=file_ids)
            documents.extend(docs)
    except TypeError as exp:
        logging.info(f"No documents retrieved from gdrive! exp: {exp}")

    node_parser = configure_node_parser(chunk_size=chunk_size)
    pg_vector = PGVectorAccess(table_name=table_name, dbname=dbname)

    documents, doc_file_ids_to_delete = check_documents(
        documents,
        community_id,
        identifier="file id",
        date_field="modified at",
        table_name=table_name,
    )
    # print("len(documents) to insert:", len(documents))
    # print("doc_file_ids_to_delete:", doc_file_ids_to_delete)

    # TODO: Delete the files with id `doc_file_ids_to_delete`

    embed_model = CohereEmbedding()

    pg_vector.save_documents_in_batches(
        community_id=community_id,
        documents=documents,
        batch_size=100,
        node_parser=node_parser,
        max_request_per_minute=None,
        embed_model=embed_model,
        embed_dim=embedding_dim,
    )


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "community_id", help="the community to save the gdrive data for it"
    )

    chinese_id = "17C7aXgnGa1v6C2wtS_tMDnaM4rBswCvO"
    italian_id = "1Czy2LYRXfctHrAv-TKoxSTb6NeImky9P"
    # portuguese_id = "1ptulSDDO1pB3f8REijnO9KlD1ytKOY2n"
    # russian_id = "1PEufDX2C4JkeFunlKK1y7nug3LbWMHc5"
    # spanish_id = "1Jrxuj92pKvGpggctKh2EjvzmfqDaoTZo"
    turkish_id = "1PnzssIXt3FgDUbhX0dioTWhY1ByYLySD"
    test_small_file_id = "115A_dZBXUZWvhos898kEoVKpZ2oyagyw"

    args = parser.parse_args()
    # process_gdrive(community_id=args.community_id, folder_id="***")
    process_gdrive(
        community_id=args.community_id,
        file_ids=[chinese_id],
        # folder_id="1IiGzpvnKSUje2jhXCEtWXINVqlcVmVst"
    )
