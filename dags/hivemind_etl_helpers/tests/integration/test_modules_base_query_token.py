from datetime import datetime, timedelta
from unittest import TestCase

from bson import ObjectId
from hivemind_etl_helpers.src.utils.modules.modules_base import ModulesBase
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestModulesBaseQueryToken(TestCase):
    def setUp(self) -> None:
        self.client = MongoSingleton.get_instance().get_client()
        self.client["Core"].drop_collection("tokens")

    def test_one_token(self):
        sample_user = ObjectId("5d7baf326c8a2e2400000000")
        sample_token_type = "type1"
        sample_token_value = "tokenid12345"

        sample_token_doc = {
            "token": sample_token_value,
            "user": sample_user,
            "type": sample_token_type,
            "expires": datetime.now() + timedelta(days=1),
            "blacklisted": False,
            "createdAt": datetime.now() - timedelta(days=1),
            "updatedAt": datetime.now() - timedelta(days=1),
        }
        self.client["Core"]["tokens"].insert_one(sample_token_doc)
        token = ModulesBase().get_token(user=sample_user, token_type=sample_token_type)

        self.assertEqual(token, sample_token_value)

    def test_empty_tokens_collection(self):
        sample_user = ObjectId("5d7baf326c8a2e2400000000")
        sample_token_type = "type1"
        with self.assertRaises(ValueError):
            _ = ModulesBase().get_token(user=sample_user, token_type=sample_token_type)

    def test_no_token(self):
        sample_user = ObjectId("5d7baf326c8a2e2400000000")
        user_with_no_token = ObjectId("5d7baf326c8a2e2400000001")
        sample_token_type = "type1"
        sample_token_value = "tokenid12345"

        sample_token_doc = {
            "token": sample_token_value,
            "user": sample_user,
            "type": sample_token_type,
            "expires": datetime.now() + timedelta(days=1),
            "blacklisted": False,
            "createdAt": datetime.now() - timedelta(days=1),
            "updatedAt": datetime.now() - timedelta(days=1),
        }
        self.client["Core"]["tokens"].insert_one(sample_token_doc)
        with self.assertRaises(ValueError):
            _ = ModulesBase().get_token(
                user=user_with_no_token, token_type=sample_token_type
            )
