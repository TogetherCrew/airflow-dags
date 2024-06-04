import unittest

from dags.analyzer_helper.discord.discord_load_transformed_members import DiscordLoadTransformedMembers
from dags.hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestDiscordLoadTransformedMembers(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = MongoSingleton.get_instance().client
        cls.platform_id = 'discord_platform'
        cls.db = cls.client[cls.platform_id]
        cls.collection = cls.db['rawmembers']

    @classmethod
    def tearDownClass(cls):
        cls.collection.delete_many({})
        cls.client.close()

    def setUp(self):
        self.collection.delete_many({})

    def test_load_recompute_true(self):
        """
        Tests that load replaces all existing data when recompute is True
        """
        initial_data = [{'_id': '1', 'data': 'initial_data_1'}, {'_id': '2', 'data': 'initial_data_2'}]
        self.collection.insert_many(initial_data)

        processed_data = [{'_id': '3', 'data': 'processed_data_1'}, {'_id': '4', 'data': 'processed_data_2'}]
        loader = DiscordLoadTransformedMembers(self.platform_id)

        loader.load(processed_data, recompute=True)

        # Verify that the collection is replaced with the processed data
        result = list(self.collection.find())
        self.assertEqual(result, processed_data)

    def test_load_recompute_false(self):
        """
        Tests that load inserts new data when recompute is False
        """
        processed_data = [{'_id': '1', 'data': 'processed_data_1'}, {'_id': '2', 'data': 'processed_data_2'}]
        loader = DiscordLoadTransformedMembers(self.platform_id)

        loader.load(processed_data, recompute=False)

        # Verify that the data is inserted
        result = list(self.collection.find())
        self.assertEqual(result, processed_data)
