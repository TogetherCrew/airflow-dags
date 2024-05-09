import unittest

from hivemind_etl_helpers.src.utils.credentials import (load_mongo_credentials,
                                                        load_redis_credentials)


class TestCredentialLoadings(unittest.TestCase):
    def test_mongo_envs_check_type(self):
        mongo_creds = load_mongo_credentials()

        self.assertIsInstance(mongo_creds, dict)

    def test_mongo_envs_values(self):
        mongo_creds = load_mongo_credentials()

        self.assertTrue(mongo_creds["user"])
        self.assertTrue(mongo_creds["password"])
        self.assertTrue(mongo_creds["host"])
        self.assertTrue(mongo_creds["port"])

        self.assertIsInstance(mongo_creds["user"], str)
        self.assertIsInstance(mongo_creds["password"], str)
        self.assertIsInstance(mongo_creds["host"], str)
        self.assertIsInstance(mongo_creds["port"], str)

    def test_redis_envs_check_type(self):
        redis_creds = load_redis_credentials()
        self.assertIsInstance(redis_creds, dict)

    def test_redis_envs_values(self):
        redis_creds = load_redis_credentials()

        self.assertTrue(redis_creds["password"])
        self.assertTrue(redis_creds["host"])
        self.assertTrue(redis_creds["port"])

        self.assertIsInstance(redis_creds["password"], str)
        self.assertIsInstance(redis_creds["host"], str)
        self.assertIsInstance(redis_creds["port"], str)
