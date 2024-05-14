import unittest

from hivemind_etl_helpers.src.utils.credentials import (
    load_mongo_credentials,
    load_qdrandt_credentials,
    load_redis_credentials,
)
from hivemind_etl_helpers.src.utils.mongo import get_mongo_uri


class TestCredentialLoadings(unittest.TestCase):
    def test_mongo_envs_check_type(self):
        mongo_creds = load_mongo_credentials()

        self.assertIsInstance(mongo_creds, dict)

    def test_mongo_envs_values(self):
        mongo_creds = load_mongo_credentials()

        self.assertNotEqual(mongo_creds["user"], None)
        self.assertNotEqual(mongo_creds["password"], None)
        self.assertNotEqual(mongo_creds["host"], None)
        self.assertNotEqual(mongo_creds["port"], None)

        self.assertIsInstance(mongo_creds["user"], str)
        self.assertIsInstance(mongo_creds["password"], str)
        self.assertIsInstance(mongo_creds["host"], str)
        self.assertIsInstance(mongo_creds["port"], str)

    def test_redis_envs_check_type(self):
        redis_creds = load_redis_credentials()

        self.assertIsInstance(redis_creds, dict)

    def test_redis_envs_values(self):
        redis_creds = load_redis_credentials()

        self.assertNotEqual(redis_creds["password"], None)
        self.assertNotEqual(redis_creds["host"], None)
        self.assertNotEqual(redis_creds["port"], None)

        self.assertIsInstance(redis_creds["password"], str)
        self.assertIsInstance(redis_creds["host"], str)
        self.assertIsInstance(redis_creds["port"], str)

    def test_load_qdrant_creds(self):
        qdrant_creds = load_qdrandt_credentials()

        self.assertIsNotNone(qdrant_creds["host"])
        self.assertIsNotNone(qdrant_creds["port"])
        self.assertIsNotNone(qdrant_creds["api_key"])

    def test_config_mongo_creds(self):
        mongo_uri = get_mongo_uri()

        self.assertIsInstance(mongo_uri, str)
        self.assertIn("mongodb://", mongo_uri)
        self.assertNotIn("None", mongo_uri)
