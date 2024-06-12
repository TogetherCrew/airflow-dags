import logging
from datetime import datetime

from analyzer_helper.discord.transform_raw_data_base import TransformRawDataBase
from analyzer_helper.discord.utils.is_user_bot import UserBotChecker


class DiscordTransformRawData(TransformRawDataBase):

    def __init__(self):
        """
        Initializes the class with a bot checker.
        """
        self.user_bot_checker = UserBotChecker()

    def create_interaction(self, interaction_type, users_engaged_id, interaction_role):
        return {
            "name": interaction_type,
            "users_engaged_id": users_engaged_id,
            "type": interaction_role,
        }

    def create_receiver_interaction(self, data, interaction_type, engaged_user, author):
        is_bot = self.user_bot_checker(author)
        return {
            "author_id": engaged_user,
            "date": data["createdDate"]["$date"],
            "source_id": data["messageId"],
            "metadata": {
                "thread_id": data["threadId"],
                "channel_id": data["channelId"],
                "bot_activity": data["isGeneratedByWebhook"] or is_bot,
            },
            "actions": [],
            "interactions": [
                {
                    "name": interaction_type,
                    "users_engaged_id": [author],
                    "type": "receiver",
                }
            ],
        }

    def create_emitter_interaction(
        self, user, period, data, interaction_type, engaged_user
    ):
        is_bot = self.user_bot_checker(engaged_user)
        return {
            "author_id": user,
            "date": period,
            "source_id": data["messageId"],
            "metadata": {
                "thread_id": data["threadId"],
                "channel_id": data["channelId"],
                "bot_activity": data["isGeneratedByWebhook"] or is_bot,
            },
            "actions": [],
            "interactions": [
                {
                    "name": interaction_type,
                    "users_engaged_id": [engaged_user],
                    "type": "emitter",
                }
            ],
        }

    def create_transformed_item(self, data, period, interactions):
        is_bot = self.user_bot_checker(data["author"])
        return {
            "author_id": data["author"],
            "date": period,
            "source_id": data["messageId"],
            "metadata": {
                "thread_id": data["threadId"],
                "channel_id": data["channelId"],
                "bot_activity": data["isGeneratedByWebhook"] or is_bot,
            },
            "actions": [
                {
                    "name": "message",
                    "type": "emitter",
                }
            ],
            "interactions": interactions,
        }

    def transform(
        self, raw_data: list, platform_id: str, period: datetime
    ) -> list[dict]:
        transformed_data = []
        for data in raw_data:
            try:
                if "author" not in data or not data["author"]:
                    raise ValueError("Missing 'author' in raw data")

                interactions = []

                if data.get("replied_user"):
                    interactions.append(
                        self.create_interaction(
                            "reply", [data["replied_user"]], "emitter"
                        )
                    )
                    receiver_interaction = self.create_receiver_interaction(
                        data, "reply", data["replied_user"], data["author"]
                    )
                    transformed_data.append(receiver_interaction)

                if data.get("user_mentions"):
                    interactions.append(
                        self.create_interaction(
                            "mention", data["user_mentions"], "emitter"
                        )
                    )
                    for mentioned_user in data["user_mentions"]:
                        mentioned_user_interaction = self.create_receiver_interaction(
                            data, "mention", mentioned_user, data["author"]
                        )
                        transformed_data.append(mentioned_user_interaction)

                all_reaction_users = []
                if data.get("reactions"):
                    for reaction in data["reactions"]:
                        parts = reaction.split(",")
                        if len(parts) > 2:
                            all_reaction_users.extend(parts[:-1])

                    if all_reaction_users:
                        interactions.append(
                            self.create_interaction(
                                "reaction", all_reaction_users, "receiver"
                            )
                        )
                        for user in all_reaction_users:
                            emitter_interaction = self.create_emitter_interaction(
                                user, period, data, "reaction", data["author"]
                            )
                            transformed_data.append(emitter_interaction)

                transformed_item = self.create_transformed_item(
                    data, period, interactions
                )
                transformed_data.append(transformed_item)
            except Exception as e:
                logging.error(f"Error transforming raw discord data. Error: {e}")

        return transformed_data
