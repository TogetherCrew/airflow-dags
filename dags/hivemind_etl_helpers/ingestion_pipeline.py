import logging

from hivemind_etl_helpers.src.utils.credentials import load_redis_credentials
from hivemind_etl_helpers.src.utils.mongo import get_mongo_uri
from hivemind_etl_helpers.src.utils.qdrant import QdrantSingleton
from hivemind_etl_helpers.src.utils.redis import RedisSingleton
from llama_index.core import Document, MockEmbedding
from llama_index.core.ingestion import (
    DocstoreStrategy,
    IngestionCache,
    IngestionPipeline,
)
from llama_index.core.node_parser import SemanticSplitterNodeParser
from llama_index.storage.docstore.mongodb import MongoDocumentStore
from llama_index.storage.kvstore.redis import RedisKVStore as RedisCache
from llama_index.vector_stores.qdrant import QdrantVectorStore
from tc_hivemind_backend.db.credentials import load_postgres_credentials
from tc_hivemind_backend.db.utils.model_hyperparams import load_model_hyperparams
from tc_hivemind_backend.embeddings.cohere import CohereEmbedding


class CustomIngestionPipeline:
    def __init__(self, community_id: str, collection_name: str, testing: bool = False):
        self.community_id = community_id
        self.qdrant_client = QdrantSingleton.get_instance().client

        _, self.embedding_dim = load_model_hyperparams()
        self.pg_creds = load_postgres_credentials()
        self.redis_cred = load_redis_credentials()
        self.collection_name = community_id
        self.platform_name = collection_name

        self.embed_model = (
            CohereEmbedding()
            if not testing
            else MockEmbedding(embed_dim=self.embedding_dim)
        )
        self.redis_client = RedisSingleton.get_instance().get_client()

    def run_pipeline(self, docs: list[Document]):
        # qdrant is just collection based and doesn't have any database
        qdrant_collection_name = f"{self.collection_name}_{self.platform_name}"
        vector_store = QdrantVectorStore(
            client=self.qdrant_client,
            collection_name=qdrant_collection_name,
        )

        pipeline = IngestionPipeline(
            transformations=[
                SemanticSplitterNodeParser(embed_model=self.embed_model),
                self.embed_model,
            ],
            docstore=MongoDocumentStore.from_uri(
                uri=get_mongo_uri(),
                db_name=f"docstore_{self.collection_name}",
                namespace=self.platform_name,
            ),
            vector_store=vector_store,
            cache=IngestionCache(
                cache=RedisCache.from_redis_client(self.redis_client),
                collection=f"{self.collection_name}_{self.platform_name}_ingestion_cache",
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
