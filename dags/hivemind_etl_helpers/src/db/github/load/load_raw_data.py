import logging
from llama_index import Document

from hivemind_etl_helpers.src.document_node_parser import configure_node_parser
from tc_hivemind_backend.db.utils.model_hyperparams import load_model_hyperparams
from tc_hivemind_backend.embeddings.cohere import CohereEmbedding
from tc_hivemind_backend.pg_vector_access import PGVectorAccess


def load_documents_into_pg_database(
    documents: list[Document],
    community_id: str,
    table_name: str,
    **kwargs,
) -> None:
    """
    load documents into a table of postgresql db
    Note: the db should be created before calling this function.

    Parameters
    -----------
    documents : list[llama_index.Document]
        the llama_index documents to save
    community_id : str
        the community id to save data within it
    table_name : str
        the table name to save the data
        for default it would be `github` which in llama_index case
        it would save within `data_github` table.
    **kwargs :
        db_name : str
            the database name to save the contents
            for default it is `"community_{community_id}"`
        deletion_query : str
            a query to delete some documents
    """
    chunk_size, embedding_dim = load_model_hyperparams()
    dbname = kwargs.get("db_name", f"community_{community_id}")

    pg_vector = PGVectorAccess(table_name=table_name, dbname=dbname)

    embed_model = CohereEmbedding()
    node_parser = configure_node_parser(chunk_size=chunk_size)

    pg_vector.save_documents_in_batches(
        community_id=community_id,
        documents=documents,
        batch_size=100,
        node_parser=node_parser,
        max_request_per_minute=None,
        embed_model=embed_model,
        embed_dim=embedding_dim,
        request_per_minute=10000,
        deletion_query=kwargs.get("deletion_query", ""),
    )
