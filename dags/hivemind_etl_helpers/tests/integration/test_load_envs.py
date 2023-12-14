import unittest

from hivemind_etl_helpers.src.utils.credentials import (
    load_mongo_credentials,
    load_postgres_credentials,
)


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

    def test_postgres_envs_type(self):
        postgres_creds = load_postgres_credentials()

        self.assertIsInstance(postgres_creds, dict)

    def test_postgres_envs_values(self):
        postgres_creds = load_postgres_credentials()

        self.assertNotEqual(postgres_creds["host"], None)
        self.assertNotEqual(postgres_creds["password"], None)
        self.assertNotEqual(postgres_creds["user"], None)
        self.assertNotEqual(postgres_creds["port"], None)
        self.assertNotEqual(postgres_creds["db_name"], None)

        self.assertIsInstance(postgres_creds["host"], str)
        self.assertIsInstance(postgres_creds["password"], str)
        self.assertIsInstance(postgres_creds["user"], str)
        self.assertIsInstance(postgres_creds["port"], str)
        self.assertIsInstance(postgres_creds["db_name"], str)
