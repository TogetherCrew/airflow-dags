import argparse
import logging

from llama_index import Document

from hivemind_etl_helpers.src.document_node_parser import configure_node_parser
from hivemind_etl_helpers.src.utils.check_documents import check_documents
from tc_hivemind_backend.db.pg_db_utils import setup_db
from tc_hivemind_backend.db.utils.model_hyperparams import load_model_hyperparams
from tc_hivemind_backend.embeddings.cohere import CohereEmbedding
from tc_hivemind_backend.pg_vector_access import PGVectorAccess


def transform_raw_to_summary_documents(documents: list[Document]):
    """
    transform_raw_to_summary_documents
    """
    raise NotImplementedError
