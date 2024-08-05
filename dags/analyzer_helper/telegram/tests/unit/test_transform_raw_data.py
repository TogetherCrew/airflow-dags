import datetime
import unittest

from analyzer_helper.telegram.transform_raw_data import TransformRawInfo
from analyzer_helper.discord.utils.is_user_bot import UserBotChecker


class TestTransformRawInfo(unittest.TestCase):
    def setUp(self):
        """Initialize the TransformRawInfo instance before each test."""
        self.transformer = TransformRawInfo()
        self.platform_id = "test_platform"

    def test_transform_data_with_single_reply(self):
        result = [
                {
                "message_id": 3.0,
                "message_text": "🎉️️️️️️ Welcome to the TC Ingestion Pipeline",
                "message_created_at": 1713037938.0,
                "user_id": 1.0,
                "reactions": [],
                "replies": [
                    {
                        "replier_id": 2.0,
                        "replied_date": 1713038036.0,
                        "reply_message_id": 5.0
                    }
                ],
                "mentions": []
            },
        ]
        expected_result = [
            {
                'author_id': '1.0',
                'date': datetime(2024, 4, 13, 21, 52, 18),
                'source_id': '3.0',
                'metadata': {
                    'category_id': None,
                    'topic_id': None,
                    'bot_activity': False
                },
                'actions': [
                    {
                        'name': 'message',
                        'type': 'emitter'
                    }
                ],
                'interactions': [
                    {
                        'name': 'reply',
                        'type': 'emitter',
                        'users_engaged_id': ['2']
                    }
                ]
            },
            {
                'author_id': '2',
                'date': datetime(2024, 4, 13, 21, 52, 18),
                'source_id': '3.0',
                'metadata': {
                    'category_id': None,
                    'topic_id': None,
                    'bot_activity': False
                },
                'actions': [],
                'interactions': [
                    {
                        'name': 'reply',
                        'type': 'receiver',
                        'users_engaged_id': ['1.0']
                    }
                ]
            }
        ]
        self.assertEqual(result, expected_result)

    def test_transform_data_with_multiple_replies(self):
        result = [
            {
                "message_id": 3.0,
                "message_text": "🎉️️️️️️ Welcome to the TC Ingestion Pipeline",
                "message_created_at": 1713037938.0,
                "user_id": 1.0,
                "reactions": [],
                "replies": [
                    {
                        "replier_id": 2.0,
                        "replied_date": 1713038036.0,
                        "reply_message_id": 5.0
                    },
                    {
                        "replier_id": 3.0,
                        "replied_date": 1713038036.0,
                        "reply_message_id": 6.0
                    },
                ],
                "mentions": []
            },
        ]
        expected_result = [
            {
                'author_id': '1.0',
                'date': datetime(2024, 4, 13, 21, 52, 18),
                'source_id': '3.0',
                'metadata': {
                    'category_id': None,
                    'topic_id': None,
                    'bot_activity': False
                },
                'actions': [
                    {
                        'name': 'message',
                        'type': 'emitter'
                    }
                ],
                'interactions': [
                    {
                        'name': 'reply',
                        'type': 'emitter',
                        'users_engaged_id': ['2']
                    },
                    {
                        'name': 'reply',
                        'type': 'emitter',
                        'users_engaged_id': ['3']
                    }
                ]
            },
            {
                'author_id': '2',
                'date': datetime(2024, 4, 13, 21, 52, 18),
                'source_id': '3.0',
                'metadata': {
                    'category_id': None,
                    'topic_id': None,
                    'bot_activity': False
                },
                'actions': [],
                'interactions': [
                    {
                        'name': 'reply',
                        'type': 'receiver',
                        'users_engaged_id': ['1.0']
                    }
                ]
            },
            {
                'author_id': '3',
                'date': datetime(2024, 4, 13, 21, 52, 18),
                'source_id': '3.0',
                'metadata': {
                    'category_id': None,
                    'topic_id': None,
                    'bot_activity': False
                },
                'actions': [],
                'interactions': [
                    {
                        'name': 'reply',
                        'type': 'receiver',
                        'users_engaged_id': ['1.0']
                    }
                ]
            }
        ]
        self.assertEqual(result, expected_result)

    def test_transform_data_with_single_mention(self):

        result = [
            {
                "message_id": 7.0,
                "message_text": "@togethercrewdev @cr3a1 🙌",
                "message_created_at": 1713038125.0,
                "user_id": 2.0,
                "reactions": [],
                "replies": [],
                "mentions": [
                    {
                        "mentioned_user_id": 3.0
                    },
                ]
            },
        ]
        expected_result = [
            {
                'author_id': '2.0',
                'date': datetime(2024, 4, 13, 21, 55, 25),
                'source_id': '7.0',
                'metadata': {
                    'category_id': None,
                    'topic_id': None,
                    'bot_activity': False
                },
                'actions': [
                    {
                        'name': 'message',
                        'type': 'emitter'
                    }
                ],
                'interactions': [
                    {
                        'name': 'mention',
                        'type': 'receiver',
                        'users_engaged_id': ['3']
                    }
                ]
            },
            {
                'author_id': '3',
                'date': datetime(2024, 4, 13, 21, 55, 25),
                'source_id': '7.0',
                'metadata': {
                    'category_id': None,
                    'topic_id': None,
                    'bot_activity': False
                },
                'actions': [],
                'interactions': [
                    {
                        'name': 'mention',
                        'type': 'emitter',
                        'users_engaged_id': ['2.0']
                    }
                ]
            }
        ]
        self.assertEqual(result, expected_result)

    def test_transform_data_with_multiple_mentions(self):
        result = [
            {
                "message_id": 7.0,
                "message_text": "@togethercrewdev @cr3a1 🙌",
                "message_created_at": 1713038125.0,
                "user_id": 1.0,
                "reactions": [],
                "replies": [],
                "mentions": [
                    {
                        "mentioned_user_id": 2.0
                    },
                    {
                        "mentioned_user_id": 3.0
                    }
                ]
            },
        ]
        expected_result = [
            {
                "author_id": "1.0",
                "date": "2024-04-13T21:55:25",
                "source_id": "7.0",
                "metadata": {
                    "category_id": None,
                    "topic_id": None,
                    "bot_activity": False,
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
                "interactions": [
                    {
                        "name": "mention",
                        "type": "receiver",
                        "users_engaged_id": [
                            "2",
                        ]
                    },
                    {
                        "name": "mention",
                        "type": "receiver",
                        "users_engaged_id": [
                            "3",
                        ]
                    }
                ]
            },
            {
                "author_id": "2",
                "date": "2024-04-13T21:55:25",
                "source_id": "7.0",
                "metadata": {
                    "category_id": None,
                    "topic_id": None,
                    "bot_activity": False,
                },
                "actions": [],
                "interactions": [
                    {
                        "name": "mention",
                        "type": "emitter",
                        "users_engaged_id": [
                            "1.0",
                        ]
                    }
                ]
            },
            {
                "author_id": "3",
                "date": "2024-04-13T21:55:25",
                "source_id": "7.0",
                "metadata": {
                    "category_id": None,
                    "topic_id": None,
                    "bot_activity": False,
                },
                "actions": [],
                "interactions": [
                    {
                        "name": "mention",
                        "type": "emitter",
                        "users_engaged_id": [
                            "1.0",
                        ]
                    }
                ]
            }
        ]
        self.assertEqual(result, expected_result)

    def test_transform_data_with_single_reaction(self):
        result = [
                {
                "message_id": 11.0,
                "message_text": "Ah I lost the chat history",
                "message_created_at": 1713038191.0,
                "user_id": 2.0,
                "reactions": [
                    {
                        "reaction": "[🍓]",
                        "reaction_date": 1713038348.0,
                        "reactor_id": 1.0
                    }
                ],
                "replies": [],
                "mentions": []
            }
        ]
        expected_result = [
            {
                'author_id': '2.0',
                'date': datetime(2024, 4, 13, 21, 56, 31),
                'source_id': '11.0',
                'metadata': {
                    'category_id': None,
                    'topic_id': None,
                    'bot_activity': False
                },
                'actions': [
                    {
                        'name': 'message',
                        'type': 'emitter'
                    }
                ],
                'interactions': [
                    {
                        'name': 'reaction',
                        'type': 'receiver',
                        'users_engaged_id': ['1']
                    }
                ]
            },
            {
                'author_id': '1',
                'date': datetime(2024, 4, 13, 21, 56, 31),
                'source_id': '11.0',
                'metadata': {
                    'category_id': None,
                    'topic_id': None,
                    'bot_activity': False
                },
                'actions': [],
                'interactions': [
                    {
                        'name': 'reaction',
                        'type': 'emitter',
                        'users_engaged_id': ['2.0']
                    }
                ]
            }
        ]
        self.assertEqual(result, expected_result)

    def test_transform_data_with_multiple_reactions(self):
        result = [
            {
                "message_id": 11.0,
                "message_text": "Ah I lost the chat history",
                "message_created_at": 1713038191.0,
                "user_id": 2.0,
                "reactions": [
                    {
                        "reaction": "[{\"type\":\"emoji\",\"emoji\":\"🍓\"}]",
                        "reaction_date": 1713038348.0,
                        "reactor_id": 1.0
                    },
                    {
                        "reaction": "[{\"type\":\"emoji\",\"emoji\":\"🙌\"}]",
                        "reaction_date": 1713038349.0,
                        "reactor_id": 2.0
                    }
                ],
                "replies": [],
                "mentions": [],
            }
        ]
        expected_result = [
            {
                'author_id': '2.0',
                'date': datetime(2024, 4, 13, 21, 56, 31),
                'source_id': '11.0',
                'metadata': {
                    'category_id': None,
                    'topic_id': None,
                    'bot_activity': False
                },
                'actions': [
                    {
                        'name': 'message',
                        'type': 'emitter'
                    }
                ],
                'interactions': [
                    {
                        'name': 'reaction',
                        'type': 'receiver',
                        'users_engaged_id': ['1']
                    },
                    {
                        'name': 'reaction',
                        'type': 'receiver',
                        'users_engaged_id': ['2']
                    }
                ]
            },
            {
                'author_id': '1',
                'date': datetime(2024, 4, 13, 21, 56, 31),
                'source_id': '11.0',
                'metadata': {
                    'category_id': None,
                    'topic_id': None,
                    'bot_activity': False
                },
                'actions': [],
                'interactions': [
                    {
                        'name': 'reaction',
                        'type': 'emitter',
                        'users_engaged_id': ['2.0']
                    }
                ]
            },
            {
                'author_id': '2',
                'date': datetime(2024, 4, 13, 21, 56, 31),
                'source_id': '11.0',
                'metadata': {
                    'category_id': None,
                    'topic_id': None,
                    'bot_activity': False
                },
                'actions': [],
                'interactions': [
                    {
                        'name': 'reaction',
                        'type': 'emitter',
                        'users_engaged_id': ['2.0']
                    }
                ]
            }
        ]
    
    def test_transform_data_with_replied_mentions_interactions(self):
        result = [
            {
                "message_id": 3.0,
                "message_text": "🎉️️️️️️ Welcome to the TC Ingestion Pipeline",
                "message_created_at": 1713037938.0,
                "user_id": 1.0,
                "reactions": [
                    {
                        "reaction": "[{\"type\":\"emoji\",\"emoji\":\"🍓\"}]",
                        "reaction_date": 1713038348.0,
                        "reactor_id": 2.0
                    },
                    {
                        "reaction": "[{\"type\":\"emoji\",\"emoji\":\"🙌\"}]",
                        "reaction_date": 1713038349.0,
                        "reactor_id": 3.0
                    }
                ],
                "replies": [
                    {
                        "replier_id": 4.0,
                        "replied_date": 1713038036.0,
                        "reply_message_id": 5.0
                    },
                    {
                        "replier_id": 5.0,
                        "replied_date": 1713038036.0,
                        "reply_message_id": 6.0
                    }
                ],
                "mentions": [
                    {
                        "mentioned_user_id": 6.0
                    },
                    {
                        "mentioned_user_id": 7.0
                    }
                ]
            }
        ]
        expected_result = [
            {
                'author_id': '1.0',
                'date': datetime(2024, 4, 13, 21, 52, 18),
                'source_id': '3.0',
                'metadata': {
                    'category_id': None,
                    'topic_id': None,
                    'bot_activity': False
                },
                'actions': [
                    {
                        'name': 'message',
                        'type': 'emitter'
                    }
                ],
                'interactions': [
                    {
                        'name': 'reaction',
                        'type': 'receiver',
                        'users_engaged_id': ['2']
                    },
                    {
                        'name': 'reaction',
                        'type': 'receiver',
                        'users_engaged_id': ['3']
                    },
                    {
                        'name': 'reply',
                        'type': 'emitter',
                        'users_engaged_id': ['4']
                    },
                    {
                        'name': 'reply',
                        'type': 'emitter',
                        'users_engaged_id': ['5']
                    },
                    {
                        'name': 'mention',
                        'type': 'receiver',
                        'users_engaged_id': ['6']
                    },
                    {
                        'name': 'mention',
                        'type': 'receiver',
                        'users_engaged_id': ['7']
                    }
                ]
            },
            {
                'author_id': '2',
                'date': datetime(2024, 4, 13, 21, 52, 18),
                'source_id': '3.0',
                'metadata': {
                    'category_id': None,
                    'topic_id': None,
                    'bot_activity': False
                },
                'actions': [],
                'interactions': [
                    {
                        'name': 'reaction',
                        'type': 'emitter',
                        'users_engaged_id': ['1.0']
                    }
                ]
            },
            {
                'author_id': '3',
                'date': datetime(2024, 4, 13, 21, 52, 18),
                'source_id': '3.0',
                'metadata': {
                    'category_id': None,
                    'topic_id': None,
                    'bot_activity': False
                },
                'actions': [],
                'interactions': [
                    {
                        'name': 'reaction',
                        'type': 'emitter',
                        'users_engaged_id': ['1.0']
                    }
                ]
            },
            {
                'author_id': '4',
                'date': datetime(2024, 4, 13, 21, 52, 18),
                'source_id': '3.0',
                'metadata': {
                    'category_id': None,
                    'topic_id': None,
                    'bot_activity': False
                },
                'actions': [],
                'interactions': [
                    {
                        'name': 'reply',
                        'type': 'receiver',
                        'users_engaged_id': ['1.0']
                    }
                ]
            },
            {
                'author_id': '5',
                'date': datetime(2024, 4, 13, 21, 52, 18),
                'source_id': '3.0',
                'metadata': {
                    'category_id': None,
                    'topic_id': None,
                    'bot_activity': False
                },
                'actions': [],
                'interactions': [
                    {
                        'name': 'reply',
                        'type': 'receiver',
                        'users_engaged_id': ['1.0']
                    }
                ]
            },
            {
                'author_id': '6',
                'date': datetime(2024, 4, 13, 21, 52, 18),
                'source_id': '3.0',
                'metadata': {
                    'category_id': None,
                    'topic_id': None,
                    'bot_activity': False
                },
                'actions': [],
                'interactions': [
                    {
                        'name': 'mention',
                        'type': 'emitter',
                        'users_engaged_id': ['1.0']
                    }
                ]
            },
            {
                'author_id': '7',
                'date': datetime(2024, 4, 13, 21, 52, 18),
                'source_id': '3.0',
                'metadata': {
                    'category_id': None,
                    'topic_id': None,
                    'bot_activity': False
                },
                'actions': [],
                'interactions': [
                    {
                        'name': 'mention',
                        'type': 'emitter',
                        'users_engaged_id': ['1.0']
                    }
                ]
            }
        ]
        self.assertEqual(result, expected_result)

    def test_transform_data_empty(self):
        raw_data = []

        expected_result = []

        result = self.transformer.transform(
            raw_data=raw_data,
        )
        self.assertEqual(result, expected_result)