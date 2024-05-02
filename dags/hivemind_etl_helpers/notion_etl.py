import logging
import os

from dotenv import load_dotenv
from hivemind_etl_helpers.src.db.gdrive.db_utils import setup_db
from hivemind_etl_helpers.src.document_node_parser import configure_node_parser
from llama_index.core.ingestion import DocstoreStrategy, IngestionCache
from llama_index.storage.docstore.redis import RedisDocumentStore
from llama_index.storage.kvstore.redis import RedisKVStore as RedisCache
from tc_hivemind_backend.db.utils.model_hyperparams import (
    load_model_hyperparams,
)
from tc_hivemind_backend.embeddings.cohere import CohereEmbedding
from tc_hivemind_backend.pg_vector_access import PGVectorAccess

from dags.hivemind_etl_helpers.src.db.notion.extractor import NotionExtractor
from dags.hivemind_etl_helpers.src.utils.custom_ingestion_pipeline import (
    CustomIngestionPipeline,
)


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
    load_dotenv()
    chunk_size, embedding_dim = load_model_hyperparams()
    table_name = "notion"
    dbname = f"community_{community_id}"
    document_store_name = f"document_store_{community_id}"
    ingestion_cache_name = f"ingestion_cache_{community_id}"

    if database_ids is None and page_ids is None:
        raise ValueError("At least one of the `database_ids` or `page_ids` must be given!")

    setup_db(community_id=community_id)

    try:
        notion_extractor = NotionExtractor()
        documents = notion_extractor.extract(page_ids=page_ids,
                                             database_ids=database_ids)
    except TypeError as exp:
        logging.info(f"No documents retrieved from notion! exp: {exp}")

    node_parser = configure_node_parser(chunk_size=chunk_size)
    pg_vector = PGVectorAccess(table_name=table_name, dbname=dbname)
    pg_vector_index = pg_vector.setup_pgvector_index(embed_dim=embedding_dim)
    embed_model = CohereEmbedding()

    pipeline = CustomIngestionPipeline(
        transformations=[
            node_parser,
            embed_model,
        ],
        # This could be postgres as well
        docstore=RedisDocumentStore.from_host_and_port(
            os.getenv("REDIS_URL"), os.getenv("REDIS_PORT"),
            namespace=document_store_name
        ),
        vector_store=pg_vector_index,
        cache=IngestionCache(
            cache=RedisCache.from_host_and_port(os.getenv("REDIS_URL"),
                                                os.getenv("REDIS_PORT")),
            collection=ingestion_cache_name,
        ),
        docstore_strategy=DocstoreStrategy.UPSERTS,
    )

    pipeline.run(documents=documents)
