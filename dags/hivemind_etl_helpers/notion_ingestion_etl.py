import os

from dotenv import load_dotenv
from llama_index.core import MockEmbedding
from llama_index.core.ingestion import (DocstoreStrategy, IngestionCache,
                                        IngestionPipeline)
from llama_index.core.node_parser import SemanticSplitterNodeParser
from llama_index.storage.docstore.postgres import PostgresDocumentStore
from llama_index.storage.kvstore.redis import RedisKVStore as RedisCache
from llama_index.vector_stores.postgres import PGVectorStore
from tc_hivemind_backend.embeddings.cohere import CohereEmbedding

from dags.hivemind_etl_helpers.src.db.gdrive.db_utils import setup_db


class NotionIngestionPipeline:
    def __init__(self, community_id: str, testing: bool = False):
        load_dotenv()
        host = os.getenv("POSTGRES_HOST")
        password = os.getenv("POSTGRES_PASS")
        port = os.getenv("POSTGRES_PORT")
        user = os.getenv("POSTGRES_USER")
        self.redis_host = os.getenv("REDIS_HOST")
        self.redis_port = os.getenv("REDIS_PORT")

        self.table_name = "notion"
        self.dbname = f"community_{community_id}"
        self.db_config = {
            "host": host,
            "password": password,
            "port": port,
            "user": user,
        }
        self.community_id = community_id
        self.embed_model = CohereEmbedding()
        if testing:
            self.embed_model = MockEmbedding(embed_dim=1024)

    def run_pipeline(self, docs):
        setup_db(community_id=self.community_id)
        pipeline = IngestionPipeline(
            transformations=[
                SemanticSplitterNodeParser(embed_model=self.embed_model),
                self.embed_model,
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
                cache=RedisCache.from_host_and_port(self.redis_host,
                                                    self.redis_port),
                collection=self.dbname + "_ingestion_cache",
            ),
            docstore_strategy=DocstoreStrategy.UPSERTS,
        )

        nodes = pipeline.run(documents=docs, show_progress=True)

        return nodes
