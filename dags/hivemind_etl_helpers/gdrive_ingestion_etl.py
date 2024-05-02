from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.core.ingestion import (
    DocstoreStrategy,
    IngestionPipeline,
    IngestionCache,
)
from llama_index.storage.docstore.postgres import PostgresDocumentStore
from llama_index.storage.kvstore.postgres import PostgresKVStore as PostgresCache  
from llama_index.core.node_parser import SentenceSplitter
from tc_hivemind_backend.embeddings.cohere import CohereEmbedding
from tc_hivemind_backend.pg_vector_access import PGVectorAccess 
from llama_index.core import VectorStoreIndex
from llama_index.vector_stores.postgres import PGVectorStore
import os

from src.db.gdrive.gdrive_loader import GoogleDriveLoader

class GoogleDriveIngestionPipeline:
    def __init__(self):
        # Embedding models
        self.cohere_model = CohereEmbedding()

        # Database details
        self.table_name = table_name
        self.dbname = db_name
        self.db_config = {
            "host": host,
            "port": port, 
            "database": database,
            "user": user,
            "password": postgres
        }

    def run_pipeline(self):
        pipeline = IngestionPipeline(
            transformations = [
            SentenceSplitter(chunk_size=1024, chunk_overlap=20),
            self.cohere_model  # Use your preferred embedding model
        ],
            docstore = PostgresDocumentStore.from_params(**self.db_config),
            vector_store = PGVectorStore.from_params(
            **self.db_config,
            embed_dim=1024,
        ),
            # cache=self.cache,  # Uncomment if you want to use caching
            docstore_strategy=DocstoreStrategy.UPSERTS
        )
        # print(pipeline)
        

        return pipeline
