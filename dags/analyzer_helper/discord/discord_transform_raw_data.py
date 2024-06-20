import logging
from datetime import datetime
from typing import Any, Dict, List

from analyzer_helper.discord.transform_raw_data_base import TransformRawDataBase
from analyzer_helper.discord.utils.is_user_bot import UserBotChecker


class DiscordTransformRawData(TransformRawDataBase):
    def __init__(self, platform_id: str):
        """
        Initializes the class with platform and bot checker.
        """
        self.platform_id = platform_id
        self.user_bot_checker = UserBotChecker(platform_id)

    def create_interaction_base(
        self,
        name: str,
        users_engaged_id: List[str],
        type: str,
    ) -> Dict[str, Any]:
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

    def create_interaction(
        self,
        data: Dict[str, Any],
        name: str,
        author: str,
        engaged_users: List[str],
        type: str,
    ) -> Dict[str, Any]:
        """
        Creates an interaction dictionary.

        Args:
            data (Dict[str, Any]): Raw data containing interaction details.
            name (str): Name of the interaction (e.g., 'reply', 'mention').
            author (str): ID of the author of the interaction.
            engaged_users (List[str]): List of IDs of the engaged users.
            type (str): Type of the interaction (e.g., 'receiver', 'emitter').

        Returns:
            Dict[str, Any]: Dictionary representing the interaction.
        """
        is_bot = self.user_bot_checker.is_user_bot(author)
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
                self.create_interaction_base(
                    name=name, users_engaged_id=engaged_users, type=type
                )
            ],
        }

    def create_transformed_item(self, data, period, interactions):
        is_bot = self.user_bot_checker.is_user_bot(data["author"])

        extracted_interactions = [
            interaction["interactions"] for interaction in interactions
        ]

        flat_interactions = [
            item for sublist in extracted_interactions for item in sublist
        ]

        return {
            "author_id": data["author"],
            "date": period,
            "source_id": data["messageId"],
            "metadata": {
                "thread_id": data["threadId"],
                "channel_id": data["channelId"],
                "bot_activity": data["isGeneratedByWebhook"] or is_bot,
            },
            "actions": [{"name": "message", "type": "emitter"}],
            "interactions": flat_interactions,
        }

    def transform(
        self, raw_data: list, platform_id: str, period: datetime
    ) -> List[Dict[str, Any]]:
        transformed_data = []
        for data in raw_data:
            try:
                if "author" not in data or not data["author"]:
                    raise ValueError("Missing 'author' in raw data")

                interactions = []

                if data.get("replied_user"):
                    interactions.append(
                        self.create_interaction(
                            data=data,
                            name="reply",
                            author=data["author"],
                            engaged_users=[data["replied_user"]],
                            type="emitter",
                        )
                    )
                    receiver_interaction = self.create_interaction(
                        data=data,
                        name="reply",
                        author=data["replied_user"],
                        engaged_users=[data["author"]],
                        type="receiver",
                    )
                    transformed_data.append(receiver_interaction)
                    # print(f"Added reply interaction: {receiver_interaction}")

                if data.get("user_mentions"):
                    interactions.append(
                        self.create_interaction(
                            data=data,
                            name="mention",
                            author=data["author"],
                            engaged_users=data["user_mentions"],
                            type="emitter",
                        )
                    )
                    for mentioned_user in data["user_mentions"]:
                        mentioned_user_interaction = self.create_interaction(
                            data=data,
                            name="mention",
                            author=mentioned_user,
                            engaged_users=[data["author"]],
                            type="receiver",
                        )
                        transformed_data.append(mentioned_user_interaction)
                        # print(f"Added mention interaction: {mentioned_user_interaction}")

                all_reaction_users = []
                if data.get("reactions"):
                    for reaction in data["reactions"]:
                        parts = reaction.split(",")
                        if len(parts) > 2:
                            all_reaction_users.extend(parts[:-1])

                    if all_reaction_users:
                        interactions.append(
                            self.create_interaction(
                                data=data,
                                name="reaction",
                                author=data["author"],
                                engaged_users=all_reaction_users,
                                type="receiver",
                            )
                        )
                        for user in all_reaction_users:
                            emitter_interaction = self.create_interaction(
                                data=data,
                                name="reaction",
                                author=user,
                                engaged_users=[data["author"]],
                                type="emitter",
                            )
                            transformed_data.append(emitter_interaction)
                            # print(f"Added reaction interaction: {emitter_interaction}")
                # print(f"Type of interactions: {type(interactions)}")
                # print(f"Content of interactions: {interactions}")
                transformed_item = self.create_transformed_item(
                    data=data, period=period, interactions=interactions
                )
                transformed_data.append(transformed_item)
                # print(f"Added transformed item: {transformed_item}")
            except Exception as e:
                logging.error(f"Error transforming raw discord data. Error: {e}")
        # print(f"Final transformed data: {transformed_data}")
        return transformed_data
