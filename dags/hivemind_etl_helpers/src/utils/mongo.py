import logging
from typing import Any

from pymongo import MongoClient

from .credentials import load_mongo_credentials


class MongoSingleton:
    __instance = None

    def __init__(self):
        if MongoSingleton.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            connection_uri = get_mongo_uri()
            self.client = MongoClient(connection_uri)
            MongoSingleton.__instance = self

    @staticmethod
    def get_instance():
        if MongoSingleton.__instance is None:
            MongoSingleton()
            try:
                info = MongoSingleton.__instance.client.server_info()
                logging.info(f"MongoDB Connected Successfully! server info: {info}")
            except Exception as exp:
                logging.error(f"MongoDB not connected! exp: {exp}")

        return MongoSingleton.__instance

    def get_client(self):
        return self.client


def get_mongo_uri() -> str:
    mongo_creds = load_mongo_credentials()
    user = mongo_creds["user"]
    password = mongo_creds["password"]
    host = mongo_creds["host"]
    port = mongo_creds["port"]

    connection = f"mongodb://{user}:{password}@{host}:{port}"

    return connection
