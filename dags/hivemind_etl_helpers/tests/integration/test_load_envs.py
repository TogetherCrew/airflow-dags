import unittest

from hivemind_etl_helpers.src.utils.credentials import load_mongo_credentials


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
