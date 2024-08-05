from analyzer_helper.telegram.utils.date_time_format_converter import (
    DateTimeFormatConverter,
)

class TransformRawInfo:
    def __init__(self):
        self.converter = DateTimeFormatConverter

    def create_data_entry(
        self, raw_data: dict, interaction_type: str = None, interaction_user: int = None
    ) -> dict:
        metadata = {
            "category_id": raw_data.get("category_id") if raw_data.get("category_id") not in [None, ""] else None,
            "topic_id": raw_data.get("topic_id") if raw_data.get("topic_id") not in [None, ""] else None,
            "bot_activity": False, #TODO: We need to fetch this
        }

        result = {
            "author_id": str(
                interaction_user
                if interaction_type in ["reply", "mention"]
                else raw_data.get("user_id")
            ),
            "date": self.converter.timestamp_to_datetime(raw_data.get("message_created_at")),
            "source_id": str(raw_data["message_id"]),
            "metadata": metadata,
            "actions": [],
            "interactions": [],
        }

        if interaction_type == "reaction":
            result["actions"] = []
            result["interactions"] = [
                {
                    "name": "reaction",
                    "type": "emitter",
                    "users_engaged_id": [str(raw_data["user_id"])],
                }
            ]
            result["author_id"] = str(interaction_user)
        elif interaction_type == "reply":
            result["actions"] = []
            result["interactions"] = [
                {
                    "name": "reply",
                    "type": "receiver",
                    "users_engaged_id": [str(raw_data["user_id"])],
                }
            ]
            result["author_id"] = str(interaction_user)
        elif interaction_type == "mention":
            result["actions"] = []
            result["interactions"] = [
                {
                    "name": "mention",
                    "type": "emitter",
                    "users_engaged_id": [str(raw_data["user_id"])],
                }
            ]
            result["author_id"] = str(interaction_user)
        else:
            for reaction in raw_data["reactions"]:
                if reaction["reactor_id"] is not None:
                    result["interactions"].append(
                        {
                            "name": "reaction",
                            "type": "receiver",
                            "users_engaged_id": [str(int(reaction["reactor_id"]))],
                        }
                    )
            for reply in raw_data["replies"]:
                if reply["replier_id"] is not None:
                    result["interactions"].append(
                        {
                            "name": "reply",
                            "type": "emitter",
                            "users_engaged_id": [str(int(reply["replier_id"]))],
                        }
                    )
            for mention in raw_data["mentions"]:
                if mention["mentioned_user_id"] is not None:
                    result["interactions"].append(
                        {
                            "name": "mention",
                            "type": "receiver",
                            "users_engaged_id": [str(int(mention["mentioned_user_id"]))],
                        }
                    )
            result["actions"] = [{"name": "message", "type": "emitter"}]

        return result

    def transform(self, raw_data: list) -> list:
        transformed_data = []
        for entry in raw_data:
            # Create main post entry
            transformed_data.append(self.create_data_entry(entry))

            # Create entries for reactions
            for reaction in entry["reactions"]:
                if reaction["reactor_id"] is not None:
                    transformed_data.append(
                        self.create_data_entry(
                            entry,
                            interaction_type="reaction",
                            interaction_user=int(reaction["reactor_id"]),
                        )
                    )

            # Create entries for replies
            for reply in entry["replies"]:
                if reply["replier_id"] is not None:
                    transformed_data.append(
                        self.create_data_entry(
                            entry,
                            interaction_type="reply",
                            interaction_user=int(reply["replier_id"]),
                        )
                    )

            # Create entries for mentions
            for mention in entry["mentions"]:
                if mention["mentioned_user_id"] is not None:
                    transformed_data.append(
                        self.create_data_entry(
                            entry,
                            interaction_type="mention",
                            interaction_user=int(mention["mentioned_user_id"]),
                        )
                    )

        return transformed_data
    

transformer = TransformRawInfo()
data = [
    {
        "message_id": 3.0,
        "message_text": "🎉️️️️️️ Welcome to the TC Ingestion Pipeline",
        "message_created_at": 1713037938.0,
        "user_id": 927814807.0,
        "reactions": [
            {
                "reaction": "[{\"type\":\"emoji\",\"emoji\":\"🍓\"}]",
                "reaction_date": 1713165102.0,
                "reactor_id": 265278326.0
            }
        ],
        "replies": [
            {
                "replier_id": 203678862.0,
                "replied_date": 1713038036.0,
                "reply_message_id": 5.0
            }
        ],
        "mentions": []
    },
    {
        "message_id": 7.0,
        "message_text": "@togethercrewdev @cr3a1 🙌",
        "message_created_at": 1713038125.0,
        "user_id": 203678862.0,
        "reactions": [],
        "replies": [],
        "mentions": [
            {
                "mentioned_user_id": 6504405389.0
            },
            {
                "mentioned_user_id": 927814807.0
            }
        ]
    },
    {
        "message_id": 11.0,
        "message_text": "Ah I lost the chat history",
        "message_created_at": 1713038191.0,
        "user_id": 203678862.0,
        "reactions": [
            {
                "reaction": "[]",
                "reaction_date": 1713038348.0,
                "reactor_id": 927814807.0
            }
        ],
        "replies": [],
        "mentions": []
    }
]
data = [
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
            },
        ],
        "mentions": [
            {
                "mentioned_user_id": 6.0
            },
            {
                "mentioned_user_id": 7.0
            }
        ]
    },
]
result = transformer.transform(data)
print("result: \n", result)