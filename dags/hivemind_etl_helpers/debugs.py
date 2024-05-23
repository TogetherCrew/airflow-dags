import logging

from tc_hivemind_backend.db.credentials import load_qdrant_credentials


def pring_qdrant_creds():
    qdrant_creds = load_qdrant_credentials()
    logging.info(f"qdrant_creds: {qdrant_creds}")