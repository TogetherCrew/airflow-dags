import logging

from qdrant_client import QdrantClient

from .credentials import load_qdrandt_credentials


class QdrantSingleton:
    __instance = None

    def __init__(self):
        if QdrantSingleton.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            creds = load_qdrandt_credentials()

            # if no api_key was provided
            if creds["api_key"] == "":
                self.client = QdrantClient(
                    host=creds["host"],
                    port=creds["port"],
                )
            else:
                self.client = QdrantClient(
                    host=creds["host"],
                    port=creds["port"],
                    api_key=creds["api_key"],
                )

            QdrantSingleton.__instance = self

    @staticmethod
    def get_instance():
        if QdrantSingleton.__instance is None:
            QdrantSingleton()
            try:
                _ = QdrantSingleton.__instance.client.get_collections()
                logging.info("QDrant Connected Successfully!")
            except Exception as exp:
                logging.error(f"QDrant not connected! exp: {exp}")

        return QdrantSingleton.__instance

    def get_client(self):
        return self.client
