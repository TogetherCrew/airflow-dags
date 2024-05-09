import logging

from hivemind_etl_helpers.src.utils.credentials import load_redis_credentials
from llama_index.core.ingestion import (
    DocstoreStrategy,
    IngestionCache,
    IngestionPipeline,
)
from llama_index.core.node_parser import SemanticSplitterNodeParser
from llama_index.storage.docstore.postgres import PostgresDocumentStore
from llama_index.storage.kvstore.redis import RedisKVStore as RedisCache
from llama_index.vector_stores.postgres import PGVectorStore
from tc_hivemind_backend.db.credentials import load_postgres_credentials
from tc_hivemind_backend.db.utils.model_hyperparams import load_model_hyperparams
from tc_hivemind_backend.embeddings.cohere import CohereEmbedding


class GoogleDriveIngestionPipeline:
    def __init__(self, community_id: str):
        self.community_id = community_id

        # Embedding models
        self.cohere_model = CohereEmbedding()
        _, self.embedding_dim = load_model_hyperparams()
        self.pg_creds = load_postgres_credentials()
        self.redis_cred = load_redis_credentials()
        self.table_name = "gdrive"
        self.dbname = f"community_{community_id}"

        # Database details
        self.redis_host = self.redis_cred["host"]
        self.redis_port = self.redis_cred["port"]

    def run_pipeline(self, docs):
        pipeline = IngestionPipeline(
            transformations=[
                SemanticSplitterNodeParser(embed_model=self.cohere_model),
                self.cohere_model,
            ],
            docstore=PostgresDocumentStore.from_params(
                host=self.pg_creds["host"],
                port=self.pg_creds["port"],
                database=self.dbname,
                user=self.pg_creds["user"],
                password=self.pg_creds["password"],
                table_name=self.table_name + "_docstore",
            ),
            vector_store=PGVectorStore.from_params(
                host=self.pg_creds["host"],
                port=self.pg_creds["port"],
                database=self.dbname,
                user=self.pg_creds["user"],
                password=self.pg_creds["password"],
                table_name=self.table_name,
                embed_dim=self.embedding_dim,
            ),
            cache=IngestionCache(
                cache=RedisCache.from_host_and_port(self.redis_host, self.redis_port),
                collection=self.dbname + f"_{self.table_name}" + "_ingestion_cache",
                docstore_strategy=DocstoreStrategy.UPSERTS,
            ),
            docstore_strategy=DocstoreStrategy.UPSERTS,
        )
        try:
            nodes = pipeline.run(documents=docs, show_progress=True)
            return nodes
        except Exception as e:
            logging.error(
                f"An error occurred while running the pipeline: {e}", exc_info=True
            )
