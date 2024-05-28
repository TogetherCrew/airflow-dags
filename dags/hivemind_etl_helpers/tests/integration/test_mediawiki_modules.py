from datetime import datetime
from unittest import TestCase

from bson import ObjectId
from hivemind_etl_helpers.src.utils.modules import ModulesMediaWiki
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestGetMediaWikiModules(TestCase):
    def setUp(self):
        client = MongoSingleton.get_instance().client
        client["Core"].drop_collection("modules")
        client["Core"].drop_collection("platforms")
        self.client = client
        self.modules_mediawiki = ModulesMediaWiki()

    def test_get_empty_data(self):
        result = self.modules_mediawiki.get_learning_platforms()
        self.assertEqual(result, [])

    def test_get_single_data(self):
        platform_id = ObjectId("6579c364f1120850414e0dc6")
        community_id = ObjectId("6579c364f1120850414e0dc5")

        self.client["Core"]["platforms"].insert_one(
            {
                "_id": platform_id,
                "name": "mediaWiki",
                "metadata": {
                    "baseURL": "http://example.com",
                    "path": "/api",
                },
                "community": community_id,
                "disconnectedAt": None,
                "connectedAt": datetime.now(),
                "createdAt": datetime.now(),
                "updatedAt": datetime.now(),
            }
        )
        self.client["Core"]["modules"].insert_one(
            {
                "name": "hivemind",
                "community": community_id,
                "options": {
                    "platforms": [
                        {
                            "platform": platform_id,
                            "name": "mediaWiki",
                            "metadata": {
                                "pageIds": [
                                    "Main_Page",
                                    "Help:Contents",
                                    "Sandbox",
                                ],
                            },
                        }
                    ]
                },
            }
        )

        result = self.modules_mediawiki.get_learning_platforms()

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["community_id"], "6579c364f1120850414e0dc5")
        self.assertEqual(
            result[0]["page_titles"],
            [
                "Main_Page",
                "Help:Contents",
                "Sandbox",
            ],
        )
        self.assertEqual(result[0]["base_url"], "http://example.com/api")

    def test_get_mediawiki_communities_data_multiple_platforms(self):
        """
        Two mediawiki platforms for one community
        """
        platform_id1 = ObjectId("6579c364f1120850414e0dc6")
        platform_id2 = ObjectId("6579c364f1120850414e0dc7")
        community_id = ObjectId("1009c364f1120850414e0dc5")

        self.client["Core"]["modules"].insert_one(
            {
                "name": "hivemind",
                "community": community_id,
                "options": {
                    "platforms": [
                        {
                            "platform": platform_id1,
                            "name": "mediaWiki",
                            "metadata": {
                                "pageIds": [
                                    "Main_Page",
                                    "Help:Contents",
                                ],
                            },
                        },
                        {
                            "platform": platform_id2,
                            "name": "mediaWiki",
                            "metadata": {
                                "pageIds": [
                                    "Sandbox",
                                    "Wikipedia:About",
                                ],
                            },
                        },
                    ]
                },
            }
        )

        self.client["Core"]["platforms"].insert_one(
            {
                "_id": platform_id1,
                "name": "mediaWiki",
                "metadata": {
                    "baseURL": "http://example1.com",
                    "path": "/api",
                },
                "community": community_id,
                "disconnectedAt": None,
                "connectedAt": datetime.now(),
                "createdAt": datetime.now(),
                "updatedAt": datetime.now(),
            }
        )

        self.client["Core"]["platforms"].insert_one(
            {
                "_id": platform_id2,
                "name": "mediaWiki",
                "metadata": {
                    "baseURL": "http://example2.com",
                    "path": "/api",
                },
                "community": community_id,
                "disconnectedAt": None,
                "connectedAt": datetime.now(),
                "createdAt": datetime.now(),
                "updatedAt": datetime.now(),
            }
        )

        result = self.modules_mediawiki.get_learning_platforms()

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertEqual(
            result[0],
            {
                "community_id": str(community_id),
                "page_titles": [
                    "Main_Page",
                    "Help:Contents",
                ],
                "base_url": "http://example1.com/api",
            },
        )
        self.assertEqual(
            result[1],
            {
                "community_id": str(community_id),
                "page_titles": [
                    "Sandbox",
                    "Wikipedia:About",
                ],
                "base_url": "http://example2.com/api",
            },
        )
