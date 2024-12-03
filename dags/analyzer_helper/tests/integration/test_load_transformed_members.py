import unittest

from analyzer_helper.common.load_transformed_members import LoadTransformedMembers
from dotenv import load_dotenv


class TestLoadTransformedMembersIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Connect to MongoDB - update connection string if needed
        load_dotenv()
        cls.platform_id = "test_platform"
        cls.loader = LoadTransformedMembers(cls.platform_id)

        # Ensure we're using test database
        assert "test" in cls.loader.db.name.lower(), "Must use test database!"

    def setUp(self):
        # Sample test data
        self.test_data = [
            {
                "id": "1464",
                "is_bot": False,
                "left_at": None,
                "joined_at": {"$date": "2024-11-30T16:55:31.170Z"},
                "options": {
                    "name": "User",
                    "username": "silostack",
                    "avatar": "https://forum.solana.com/user_avatar/forum.solana.com/silostack/128/641_2.png",
                },
            },
            {
                "id": "1465",
                "is_bot": False,
                "left_at": None,
                "joined_at": {"$date": "2024-11-30T17:00:00.000Z"},
                "options": {
                    "name": "Alice",
                    "username": "alice123",
                    "avatar": "https://example.com/avatar.png",
                },
            },
        ]

        # Clear the collection before each test
        self.loader.collection.delete_many({})

    def test_load_with_recompute(self):
        # First insert
        self.loader.load(self.test_data, recompute=True)

        # Verify count
        count = self.loader.collection.count_documents({})
        self.assertEqual(count, len(self.test_data))

        # Verify data
        for doc in self.test_data:
            stored_doc = self.loader.collection.find_one({"id": doc["id"]})
            self.assertIsNotNone(stored_doc)
            self.assertEqual(stored_doc["options"]["name"], doc["options"]["name"])

        # Update one record
        modified_data = self.test_data.copy()
        modified_data[0]["options"]["name"] = "User Updated"

        # Recompute should replace all data
        self.loader.load(modified_data, recompute=True)

        # Verify update
        updated_doc = self.loader.collection.find_one({"id": "1464"})
        self.assertEqual(updated_doc["options"]["name"], "User Updated")

    def test_load_without_recompute(self):
        # Initial insert
        self.loader.load([self.test_data[0]], recompute=False)

        # Verify initial insert
        count = self.loader.collection.count_documents({})
        self.assertEqual(count, 1)

        # Update existing and add new
        modified_data = self.test_data.copy()
        modified_data[0]["options"]["name"] = "User Modified"
        self.loader.load(modified_data, recompute=False)

        # Verify count after update
        count = self.loader.collection.count_documents({})
        self.assertEqual(count, len(modified_data))

        # Verify update
        updated_doc = self.loader.collection.find_one({"id": "1464"})
        self.assertEqual(updated_doc["options"]["name"], "User Modified")

        # Verify new insert
        new_doc = self.loader.collection.find_one({"id": "1465"})
        self.assertIsNotNone(new_doc)
        self.assertEqual(new_doc["options"]["name"], "Alice")

    def test_duplicate_inserts(self):
        # Insert same data twice without recompute
        self.loader.load(self.test_data, recompute=False)
        self.loader.load(self.test_data, recompute=False)

        # Verify no duplicates
        count = self.loader.collection.count_documents({})
        self.assertEqual(count, len(self.test_data))

    def test_large_batch(self):
        # Create larger dataset
        large_dataset = []
        for i in range(1000):
            doc = {
                "id": str(i),
                "is_bot": False,
                "left_at": None,
                "joined_at": {"$date": "2024-11-30T16:55:31.170Z"},
                "options": {
                    "name": f"User{i}",
                    "username": f"user{i}",
                    "avatar": f"https://example.com/avatar{i}.png",
                },
            }
            large_dataset.append(doc)

        # Test insert
        self.loader.load(large_dataset, recompute=False)

        # Verify count
        count = self.loader.collection.count_documents({})
        self.assertEqual(count, len(large_dataset))

    def tearDown(self):
        # Clean up after each test
        self.loader.collection.delete_many({})

    @classmethod
    def tearDownClass(cls):
        # Optional: drop the test database after all tests
        cls.loader.client.drop_database(cls.loader.db.name)
