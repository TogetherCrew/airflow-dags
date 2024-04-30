import argparse
import logging

from tc_hivemind_backend.db.utils.model_hyperparams import load_model_hyperparams
from dags.hivemind_etl_helpers.src.utils.custom_ingestion_pipeline import CustomIngestionPipeline
from hivemind_etl_helpers.src.utils.check_documents import check_documents
from dags.hivemind_etl_helpers.src.db.notion.extractor import NotionExtractor
from hivemind_etl_helpers.src.document_node_parser import configure_node_parser
from hivemind_etl_helpers.src.db.gdrive.db_utils import setup_db
from tc_hivemind_backend.pg_vector_access import PGVectorAccess
from tc_hivemind_backend.embeddings.cohere import CohereEmbedding
from llama_index.storage.docstore.redis import RedisDocumentStore
from llama_index.core.ingestion import (
    DocstoreStrategy,
    IngestionCache,
)
from llama_index.storage.kvstore.redis import RedisKVStore as RedisCache


def process_notion_etl(
    community_id: str, database_ids: list[str] | None = None,
    page_ids: list[str] | None = None
) -> None:
    """
    process the notion files
    and save the processed data within postgresql

    Parameters
    -----------
    community_id : str
        the community to save its data
    database_ids : list[str] | None
        the database ids to process its data
        default is None
    page_ids : list[str] | None
        the page ids to process their data
        default is None

    Note: One of `database_ids` or `page_ids` should be given.
    """
    chunk_size, embedding_dim = load_model_hyperparams()
    table_name = "notion"
    dbname = f"community_{community_id}"

    if database_ids is None and page_ids is None:
        raise ValueError("At least one of the `database_ids` or `page_ids` must be given!")

    setup_db(community_id=community_id)

    try:
        notion_extractor = NotionExtractor(integration_token="your_token_here")
        documents = notion_extractor.extract(page_ids=page_ids,
                                             database_ids=database_ids)
    except TypeError as exp:
        logging.info(f"No documents retrieved from notion! exp: {exp}")

    node_parser = configure_node_parser(chunk_size=chunk_size)
    pg_vector = PGVectorAccess(table_name=table_name, dbname=dbname)
    pg_vector_index = pg_vector.setup_pgvector_index(embed_dim=embedding_dim)

    # print("len(documents) to insert:", len(documents))
    # print("doc_file_ids_to_delete:", doc_file_ids_to_delete)

    # TODO: Delete the files with id `doc_file_ids_to_delete`

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

    # TODO: Create class for IngestionPipeline
    pipeline = CustomIngestionPipeline(
        transformations=[
            node_parser,
            embed_model,
        ],
        # This could be postgres as well
        docstore=RedisDocumentStore.from_host_and_port(
            "localhost", 6379, namespace="document_store"
        ),
        vector_store=pg_vector_index,
        cache=IngestionCache(
            cache=RedisCache.from_host_and_port("localhost", 6379),
            collection="redis_cache",
        ),
        docstore_strategy=DocstoreStrategy.UPSERTS,
    )

    pipeline.run(documents=documents)
