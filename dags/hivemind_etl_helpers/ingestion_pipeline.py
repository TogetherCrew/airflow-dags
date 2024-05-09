from hivemind_etl_helpers.src.utils.credentials import load_redis_credentials
from llama_index.core import MockEmbedding
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

from dags.hivemind_etl_helpers.src.db.gdrive.db_utils import setup_db


class CustomIngestionPipeline:
    def __init__(self, community_id: str, table_name: str, testing: bool = False):
        redis_credentials = load_redis_credentials()
        postgres_credentials = load_postgres_credentials()
        self.redis_host = redis_credentials["host"]
        self.redis_port = redis_credentials["port"]
        self.table_name = table_name
        self.dbname = f"community_{community_id}"
        self.postgres_host = postgres_credentials["host"]
        self.postgres_port = postgres_credentials["port"]
        self.postgres_user = postgres_credentials["user"]
        self.postgres_password = postgres_credentials["password"]
        self.community_id = community_id
        self.embed_model = CohereEmbedding()
        if testing:
            self.embed_model = MockEmbedding(embed_dim=1024)

    def run_pipeline(self, docs):
        _, embedding_dim = load_model_hyperparams()
        setup_db(community_id=self.community_id)
        pipeline = IngestionPipeline(
            transformations=[
                SemanticSplitterNodeParser(embed_model=self.embed_model),
                self.embed_model,
            ],
            docstore=PostgresDocumentStore.from_params(
                host=self.postgres_host,
                port=self.postgres_port,
                user=self.postgres_user,
                password=self.postgres_password,
                database=self.dbname,
                table_name=self.table_name + "_docstore",
            ),
            vector_store=PGVectorStore.from_params(
                host=self.postgres_host,
                port=self.postgres_port,
                user=self.postgres_user,
                password=self.postgres_password,
                database=self.dbname,
                table_name=self.table_name,
                embed_dim=embedding_dim,
            ),
            cache=IngestionCache(
                cache=RedisCache.from_host_and_port(self.redis_host, self.redis_port),
                collection=self.dbname + f"_{self.table_name}" + "_ingestion_cache",
            ),
            docstore_strategy=DocstoreStrategy.UPSERTS,
        )

        nodes = pipeline.run(documents=docs, show_progress=True)

        return nodes
