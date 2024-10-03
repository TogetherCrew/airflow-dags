from analyzer_helper.telegram.utils.date_time_format_converter import (
    DateTimeFormatConverter,
)
from analyzer_helper.telegram.utils.is_user_bot import UserBotChecker


class TransformRawInfo:
    def __init__(self, chat_id: str):
        self.converter = DateTimeFormatConverter
        self.user_bot_checker = UserBotChecker()
        self.chat_id = chat_id

    def create_data_entry(
        self, raw_data: dict, interaction_type: str = None, interaction_user: str = None
    ) -> dict:
        author_id = str(
            int(
                interaction_user
                if interaction_type in ["reply", "mention"]
                else raw_data.get("author_id")
            )
        )
        is_bot = self.user_bot_checker.is_user_bot(str(int(author_id)))
        metadata = {
            "chat_id": self.chat_id,
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
                    "users_engaged_id": [str(int(raw_data["author_id"]))],
                }
            ]
            result["author_id"] = interaction_user
        elif interaction_type == "reply":
            result["actions"] = []
            result["interactions"] = [
                {
                    "name": "reply",
                    "type": "receiver",
                    "users_engaged_id": [str(int(raw_data["author_id"]))],
                }
            ]
            result["author_id"] = interaction_user
        elif interaction_type == "mention":
            result["actions"] = []
            result["interactions"] = [
                {
                    "name": "mention",
                    "type": "emitter",
                    "users_engaged_id": [str(int(raw_data["author_id"]))],
                }
            ]
            result["author_id"] = interaction_user
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
                            interaction_user=str(int(reaction["reactor_id"])),
                        )
                    )

            # Create entries for replies
            for reply in entry["replies"]:
                if reply["replier_id"] is not None:
                    transformed_data.append(
                        self.create_data_entry(
                            entry,
                            interaction_type="reply",
                            interaction_user=str(int(reply["replier_id"])),
                        )
                    )

            # Create entries for mentions
            for mention in entry["mentions"]:
                if mention["mentioned_user_id"] is not None:
                    transformed_data.append(
                        self.create_data_entry(
                            entry,
                            interaction_type="mention",
                            interaction_user=str(int(mention["mentioned_user_id"])),
                        )
                    )

        return transformed_data
