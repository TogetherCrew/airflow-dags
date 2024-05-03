from llama_index.core.ingestion import (
    DocstoreStrategy,
    IngestionPipeline,
    IngestionCache,
)
from llama_index.core.node_parser import SemanticSplitterNodeParser
from llama_index.storage.kvstore.redis import RedisKVStore as RedisCache

from llama_index.storage.docstore.postgres import PostgresDocumentStore
from tc_hivemind_backend.embeddings.cohere import CohereEmbedding
from llama_index.vector_stores.postgres import PGVectorStore
import os

from dotenv import load_dotenv

load_dotenv()


class GoogleDriveIngestionPipeline:
    def __init__(self, community_id: str):
        self.community_id = community_id

        # Embedding models
        self.cohere_model = CohereEmbedding()

        # Database details
        host = os.getenv("POSTGRES_HOST")
        password = os.getenv("POSTGRES_PASS")
        port = os.getenv("POSTGRES_PORT")
        user = os.getenv("POSTGRES_USER")
        self.redis_host = os.getenv("REDIS_HOST")
        self.redis_port = os.getenv("REDIS_PORT")

        self.table_name = "gdrive"
        self.dbname = f"community_{community_id}"
        self.db_config = {
            "host": host,
            "password": password,
            "port": port,
            "user": user,
        }

    def run_pipeline(self, docs):
        pipeline = IngestionPipeline(
            transformations=[
                SemanticSplitterNodeParser(embed_model=CohereEmbedding()),
                self.cohere_model,
            ],
            docstore=PostgresDocumentStore.from_params(
                **self.db_config,
                database=self.dbname,
                table_name=self.table_name + "_docstore",
            ),
            vector_store=PGVectorStore.from_params(
                **self.db_config,
                database=self.dbname,
                table_name=self.table_name,
                embed_dim=1024,
            ),
            cache=IngestionCache(
                cache=RedisCache.from_host_and_port(self.redis_host, self.redis_port),
                collection="sample_test_cache",
                docstore_strategy=DocstoreStrategy.UPSERTS,
            ),
            docstore_strategy=DocstoreStrategy.UPSERTS,
        )

        nodes = pipeline.run(documents=docs, show_progress=True)

        return nodes

