import logging
from datetime import datetime
from typing import Dict, List

from analyzer_helper.discord.transform_raw_data_base import TransformRawDataBase
from analyzer_helper.discord.utils.is_user_bot import UserBotChecker


class DiscordTransformRawData(TransformRawDataBase):
    def __init__(self):
        """
        Initializes the class with a bot checker.
        """
        self.user_bot_checker = UserBotChecker()

    def create_interaction_base(self, name: str, users_engaged_id: List[str], type: str) -> Dict[str, str]:
        """
        Creates an interaction dictionary.

        Args:
            interaction_type (str): Name of the interaction (e.g., 'reply', 'mention').
            users_engaged_id (List[str]): List of user IDs engaged in the interaction.
            type (str): Type of the interaction (e.g., 'emitter', 'receiver').

        Returns:
            Dict[str, str]: Dictionary representing the interaction.
        """
        return {
            "name": name,
            "users_engaged_id": users_engaged_id,
            "type": type,
        }

    def create_interaction(self, data: Dict[str, any], name: str, author: str, engaged_users: List[str], type: str) -> Dict[str, any]:
        """
        Creates an interaction dictionary.

        Args:
            data (Dict[str, any]): Raw data containing interaction details.
            name (str): Name of the interaction (e.g., 'reply', 'mention').
            author (str): ID of the author of the interaction.
            engaged_users (List[str]): List of IDs of the engaged users.
            type (str): Type of the interaction (e.g., 'receiver', 'emitter').

        Returns:
            Dict[str, any]: Dictionary representing the interaction.
        """
        is_bot = self.user_bot_checker(author)
        return {
            "author_id": author,
            "date": data.get("createdDate", data.get("period")),
            "source_id": data["messageId"],
            "metadata": {
                "thread_id": data["threadId"],
                "channel_id": data["channelId"],
                "bot_activity": data["isGeneratedByWebhook"] or is_bot,
            },
            "actions": [],
            "interactions": [
                self.create_interaction_base(name=name, users_engaged_id=engaged_users, type=type)
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

    def transform(self, raw_data: list, platform_id: str, period: datetime) -> list[dict]:
        transformed_data = []
        for data in raw_data:
            try:
                if "author" not in data or not data["author"]:
                    raise ValueError("Missing 'author' in raw data")

                interactions = []

                if data.get("replied_user"):
                    interactions.append(
                        self.create_interaction(
                            data=data, name="reply", author=data["author"], engaged_users=[data["replied_user"]], interaction_type="emitter"
                        )
                    )
                    receiver_interaction = self.create_interaction(
                        data=data, name="reply", author=data["author"], engaged_users=[data["replied_user"]], interaction_type="receiver"
                    )
                    transformed_data.append(receiver_interaction)

                if data.get("user_mentions"):
                    interactions.append(
                        self.create_interaction(
                            data=data, name="mention", author=data["author"], engaged_users=data["user_mentions"], interaction_type="emitter"
                        )
                    )
                    for mentioned_user in data["user_mentions"]:
                        mentioned_user_interaction = self.create_interaction(
                            data=data, name="mention", author=mentioned_user, engaged_users=[data["author"]], interaction_type="receiver"
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
                                data=data, name="reaction", author=data["author"], engaged_users=all_reaction_users, interaction_type="receiver"
                            )
                        )
                        for user in all_reaction_users:
                            emitter_interaction = self.create_interaction(
                                data=data, name="reaction", author=user, engaged_users=[data["author"]], interaction_type="emitter"
                            )
                            transformed_data.append(emitter_interaction)

                transformed_item = self.create_transformed_item(
                    data=data, period=period, interactions=interactions
                )
                transformed_data.append(transformed_item)
            except Exception as e:
                logging.error(f"Error transforming raw discord data. Error: {e}")

        return transformed_data
