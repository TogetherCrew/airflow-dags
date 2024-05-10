from dags.hivemind_etl_helpers.src.db.gdrive.db_utils import setup_db
from dags.hivemind_etl_helpers.src.utils.redis import RedisSingleton
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


class CustomIngestionPipeline:
    def __init__(self, community_id: str, table_name: str, testing: bool = False):
        self.postgres_credentials = load_postgres_credentials()
        self.table_name = table_name
        self.dbname = f"community_{community_id}"
        self.community_id = community_id
        self.embed_model = (
            CohereEmbedding() if not testing else MockEmbedding(embed_dim=1024)
        )
        self.redis_client = RedisSingleton.get_instance().get_client()
        self.redis_client = RedisSingleton.get_instance().get_client()

    def run_pipeline(self, docs):
        _, embedding_dim = load_model_hyperparams()
        setup_db(community_id=self.community_id)
        pipeline = IngestionPipeline(
            transformations=[
                SemanticSplitterNodeParser(embed_model=self.embed_model),
                self.embed_model,
            ],
            docstore=PostgresDocumentStore.from_params(
                host=self.postgres_credentials["host"],
                port=self.postgres_credentials["port"],
                user=self.postgres_credentials["user"],
                password=self.postgres_credentials["password"],
                database=self.dbname,
                table_name=self.table_name + "_docstore",
            ),
            vector_store=PGVectorStore.from_params(
                host=self.postgres_credentials["host"],
                port=self.postgres_credentials["port"],
                user=self.postgres_credentials["user"],
                password=self.postgres_credentials["password"],
                database=self.dbname,
                table_name=self.table_name,
                embed_dim=embedding_dim,
            ),
            cache=IngestionCache(
                cache=RedisCache.from_redis_client(self.redis_client),
                collection=self.dbname + f"_{self.table_name}" + "_ingestion_cache",
            ),
            docstore_strategy=DocstoreStrategy.UPSERTS,
        )

        nodes = pipeline.run(documents=docs, show_progress=True)

        return nodes
