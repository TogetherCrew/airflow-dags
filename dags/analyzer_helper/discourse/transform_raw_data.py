import logging

from analyzer_helper.discourse.utils.convert_date_time_formats import (
    DateTimeFormatConverter,
)


class TransformRawInfo:
    def __init__(self, forum_endpoint: str):
        self.forum_endpoint = forum_endpoint
        self.converter = DateTimeFormatConverter()

    def create_data_entry(
        self, raw_data: dict, interaction_type: str = None, interaction_user: int = None
    ) -> dict:
        topic_id = raw_data.get("topic_id")
        post_number = raw_data.get("post_number")
        metadata = {
            "category_id": raw_data.get("category_id"),
            "topic_id": topic_id,
            "bot_activity": False,
        }

        # Adding the message link to metadata
        if topic_id and post_number:
            metadata = {
                **metadata,  # previous ones
                "link": (
                    f"https://{self.forum_endpoint}/t/"
                    + f"{int(topic_id)}/{int(post_number)}"
                ),
            }

        result = {
            "author_id": str(
                int(interaction_user)
                if interaction_type == "reply"
                else int(raw_data.get("author_id"))
            ),
            "text": raw_data["text"],
            "date": self.converter.from_iso_format(raw_data.get("created_at")),
            "source_id": str(raw_data["post_id"]),
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
            result["author_id"] = str(int(interaction_user))
        elif interaction_type == "reply":
            result["actions"] = []
            result["interactions"] = [
                {
                    "name": "reply",
                    "type": "receiver",
                    "users_engaged_id": [str(int(raw_data["author_id"]))],
                }
            ]
            result["author_id"] = str(int(interaction_user))
        else:
            if raw_data["reactions"]:
                result["interactions"].append(
                    {
                        "name": "reaction",
                        "type": "receiver",
                        "users_engaged_id": [
                            str(int(reaction)) for reaction in raw_data["reactions"]
                        ],
                    }
                )
            if raw_data["replied_post_id"]:
                result["interactions"].append(
                    {
                        "name": "reply",
                        "type": "emitter",
                        "users_engaged_id": [
                            str(int(raw_data["replied_post_user_id"]))
                        ],
                    }
                )
            result["actions"] = [{"name": "message", "type": "emitter"}]

        return result

    def transform(self, raw_data: list) -> list:
        transformed_data = []
        for idx, entry in enumerate(raw_data):
            # Create main post entry
            transformed_data.append(self.create_data_entry(entry))

            # Create entries for reactions
            for reaction in entry["reactions"]:
                transformed_data.append(
                    self.create_data_entry(
                        entry,
                        interaction_type="reaction",
                        interaction_user=int(reaction),
                    )
                )

            # Create entry for reply
            if entry["replied_post_id"]:
                transformed_data.append(
                    self.create_data_entry(
                        entry,
                        interaction_type="reply",
                        interaction_user=int(entry["replied_post_user_id"]),
                    )
                )
            # TODO: Create entry for mentioned users

            logging.info(f"Preparing raw data: {idx + 1}/{len(raw_data)}")

        return transformed_data
