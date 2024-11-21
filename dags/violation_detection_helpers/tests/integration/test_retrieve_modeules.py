from datetime import datetime
from unittest import TestCase

from bson import ObjectId
from tc_hivemind_backend.db.mongo import MongoSingleton
from violation_detection_helpers.modules import ViolationDetectionModules


class TestRetrieveViolationDetectionModules(TestCase):
    def setUp(self) -> None:
        self.vd_module = ViolationDetectionModules()
        self.client = MongoSingleton.get_instance().get_client()

        self.client["Core"].drop_collection("modules")

    def tearDown(self) -> None:
        self.client["Core"].drop_collection("modules")

    def test_retrieve_no_platform(self):
        modules = self.vd_module.retrieve_platforms()
        self.assertEqual(modules, [])

    def test_retrieve_single_platform(self):
        self.client["Core"]["modules"].insert_one(
            {
                "name": "violationDetection",
                "community": ObjectId("515151515151515151515151"),
                "options": {
                    "platforms": [
                        {
                            "platform": ObjectId("515151515151515151515152"),
                            "name": "discourse",
                            "metadata": {
                                "selectedResources": ["123", "321", "221", "213"],
                                "selectedEmails": [
                                    "email1@example.com",
                                    "email2@example.com",
                                ],
                                "fromDate": datetime(2023, 1, 1),
                                "toDate": None,
                            },
                        }
                    ]
                },
                "createdAt": datetime(2023, 2, 2),
                "updatedAt": datetime(2023, 2, 2),
            }
        )

        modules = self.vd_module.retrieve_platforms()
        self.assertEqual(len(modules), 1)
        self.assertEqual(modules[0]["platform_id"], "515151515151515151515152")
        self.assertEqual(modules[0]["community"], "515151515151515151515151")
        self.assertEqual(modules[0]["resources"], ["123", "321", "221", "213"])
        self.assertEqual(
            modules[0]["selected_emails"], ["email1@example.com", "email2@example.com"]
        )
        self.assertEqual(modules[0]["from_date"], datetime(2023, 1, 1))
        self.assertEqual(modules[0]["to_date"], None)

    def test_retrieve_multiple_platform_single_community(self):
        self.client["Core"]["modules"].insert_many(
            [
                {
                    "name": "violationDetection",
                    "community": ObjectId("515151515151515151515151"),
                    "options": {
                        "platforms": [
                            {
                                "platform": ObjectId("515151515151515151515152"),
                                "name": "discourse",
                                "metadata": {
                                    "selectedResources": ["123", "321", "221", "213"],
                                    "selectedEmails": [
                                        "email1@example.com",
                                        "email2@example.com",
                                    ],
                                    "fromDate": datetime(2023, 1, 1),
                                    "toDate": None,
                                },
                            },
                            {
                                "platform": ObjectId("515151515151515151515153"),
                                "name": "telegram",
                                "metadata": {
                                    "selectedResources": ["7373", "8282", "1"],
                                    "selectedEmails": [
                                        "email4@example.com",
                                        "email5@example.com",
                                    ],
                                    "fromDate": datetime(2023, 1, 1),
                                    "toDate": datetime(2024, 1, 1),
                                },
                            },
                        ]
                    },
                    "createdAt": datetime(2023, 2, 2),
                    "updatedAt": datetime(2023, 2, 2),
                },
            ]
        )

        modules = self.vd_module.retrieve_platforms()
        self.assertEqual(len(modules), 2)
        for module in modules:
            if module["platform_id"] == "515151515151515151515152":
                self.assertEqual(module["platform_id"], "515151515151515151515152")
                self.assertEqual(module["community"], "515151515151515151515151")
                self.assertEqual(module["resources"], ["123", "321", "221", "213"])
                self.assertEqual(
                    module["selected_emails"],
                    [
                        "email1@example.com",
                        "email2@example.com",
                    ],
                )
                self.assertEqual(module["from_date"], datetime(2023, 1, 1))
                self.assertEqual(module["to_date"], None)
            elif module["platform_id"] == "515151515151515151515154":
                self.assertEqual(module["platform_id"], "515151515151515151515153")
                self.assertEqual(module["community"], "515151515151515151515154")
                self.assertEqual(module["resources"], ["7373", "8282", "12390"])
                self.assertEqual(
                    module["selected_emails"],
                    [
                        "email4@example.com",
                        "email5@example.com",
                    ],
                )
                self.assertEqual(module["from_date"], datetime(2023, 1, 1))
                self.assertEqual(module["to_date"], datetime(2024, 1, 1))

    def test_retrieve_multiple_platform_multiple_communities(self):
        self.client["Core"]["modules"].insert_many(
            [
                {
                    "name": "violationDetection",
                    "community": ObjectId("515151515151515151515151"),
                    "options": {
                        "platforms": [
                            {
                                "platform": ObjectId("515151515151515151515152"),
                                "name": "discourse",
                                "metadata": {
                                    "selectedResources": ["123", "321", "221", "213"],
                                    "selectedEmails": [
                                        "email1@example.com",
                                        "email2@example.com",
                                    ],
                                    "fromDate": datetime(2023, 1, 1),
                                    "toDate": None,
                                },
                            }
                        ]
                    },
                    "createdAt": datetime(2023, 2, 2),
                    "updatedAt": datetime(2023, 2, 2),
                },
                {
                    "name": "violationDetection",
                    "community": ObjectId("515151515151515151515154"),
                    "options": {
                        "platforms": [
                            {
                                "platform": ObjectId("515151515151515151515153"),
                                "name": "discord",
                                "metadata": {
                                    "selectedResources": ["7373", "8282"],
                                    "selectedEmails": [
                                        "email4@example.com",
                                        "email5@example.com",
                                    ],
                                    "fromDate": datetime(2023, 1, 1),
                                    "toDate": datetime(2024, 1, 1),
                                },
                            }
                        ]
                    },
                    "createdAt": datetime(2023, 2, 2),
                    "updatedAt": datetime(2023, 2, 2),
                },
            ]
        )

        modules = self.vd_module.retrieve_platforms()
        self.assertEqual(len(modules), 2)
        for module in modules:
            if module["platform_id"] == "515151515151515151515152":
                self.assertEqual(module["platform_id"], "515151515151515151515152")
                self.assertEqual(module["community"], "515151515151515151515151")
                self.assertEqual(module["resources"], ["123", "321", "221", "213"])
                self.assertEqual(
                    module["selected_emails"],
                    ["email1@example.com", "email2@example.com"],
                )
                self.assertEqual(module["from_date"], datetime(2023, 1, 1))
                self.assertEqual(module["to_date"], None)
            elif module["platform_id"] == "515151515151515151515154":
                self.assertEqual(module["platform_id"], "515151515151515151515153")
                self.assertEqual(module["community"], "515151515151515151515154")
                self.assertEqual(module["resources"], ["7373", "8282", "12390"])
                self.assertEqual(
                    module["selected_emails"],
                    ["email4@example.com", "email5@example.com"],
                )
                self.assertEqual(module["from_date"], datetime(2023, 1, 1))
                self.assertEqual(module["to_date"], datetime(2024, 1, 1))
