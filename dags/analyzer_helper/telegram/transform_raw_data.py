from analyzer_helper.telegram.utils.date_time_format_converter import (
    DateTimeFormatConverter,
)
from analyzer_helper.telegram.utils.is_user_bot import UserBotChecker


class TransformRawInfo:
    def __init__(self):
        self.converter = DateTimeFormatConverter
        self.user_bot_checker = UserBotChecker()

    def create_data_entry(
        self, raw_data: dict, interaction_type: str = None, interaction_user: int = None
    ) -> dict:
        author_id = str(
            interaction_user
            if interaction_type in ["reply", "mention"]
            else raw_data.get("user_id")
        )
        is_bot = self.user_bot_checker.is_user_bot(float(author_id))
        metadata = {
            "category_id": (
                raw_data.get("category_id")
                if raw_data.get("category_id") not in [None, ""]
                else None
            ),
            "topic_id": (
                raw_data.get("topic_id")
                if raw_data.get("topic_id") not in [None, ""]
                else None
            ),
            "bot_activity": is_bot,
        }
        result = {
            "author_id": author_id,
            "date": self.converter.timestamp_to_datetime(
                raw_data.get("message_created_at")
            ),
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
                            "users_engaged_id": [
                                str(int(mention["mentioned_user_id"]))
                            ],
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
