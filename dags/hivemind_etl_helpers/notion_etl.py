import logging
import os

from dotenv import load_dotenv
from hivemind_etl_helpers.src.db.gdrive.db_utils import setup_db
from llama_index.core.ingestion import (DocstoreStrategy, IngestionCache,
                                        IngestionPipeline)
from llama_index.core.node_parser import SemanticSplitterNodeParser
from llama_index.storage.docstore.postgres import PostgresDocumentStore
from llama_index.storage.kvstore.redis import RedisKVStore as RedisCache
from tc_hivemind_backend.db.utils.model_hyperparams import (
    load_model_hyperparams
)
from tc_hivemind_backend.embeddings.cohere import CohereEmbedding
from tc_hivemind_backend.pg_vector_access import PGVectorAccess

from dags.hivemind_etl_helpers.src.db.notion.extractor import NotionExtractor


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
    db_name = f"notion_community_{community_id}"
    db_name_document_store = f"notion_community_{community_id}_document_store"
    ingestion_cache_name = f"{community_id}_notion_ingestion_cache"

    if database_ids is None and page_ids is None:
        raise ValueError("At least one of the `database_ids` or `page_ids` must be given!")

    setup_db(community_id=community_id)

    try:
        notion_extractor = NotionExtractor()
        documents = notion_extractor.extract(page_ids=page_ids,
                                             database_ids=database_ids)
    except TypeError as exp:
        logging.info(f"No documents retrieved from notion! exp: {exp}")

    pg_vector = PGVectorAccess(table_name=table_name, dbname=db_name)
    pg_vector_index = pg_vector.setup_pgvector_index(embed_dim=embedding_dim)
    embed_model = CohereEmbedding()

    # Postgres document store
    doc_store = PostgresDocumentStore.from_params(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASS"),
        database=db_name_document_store,
        table_name=table_name + "_docstore"
    )

    pipeline = IngestionPipeline(
        transformations=[
            SemanticSplitterNodeParser(embed_model=embed_model),
            embed_model,
        ],
        docstore=doc_store,
        vector_store=pg_vector_index,
        cache=IngestionCache(
            cache=RedisCache.from_host_and_port(
                os.getenv("REDIS_URL", "localhost"),
                os.getenv("REDIS_PORT", 6379)),
            collection=ingestion_cache_name,
        ),
        docstore_strategy=DocstoreStrategy.UPSERTS,
    )

    nodes = pipeline.run(
        documents=documents,
        show_progress=True,
    )
