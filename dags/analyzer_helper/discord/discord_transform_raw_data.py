import logging
from typing import Any, Dict, List

from analyzer_helper.discord.transform_raw_data_base import TransformRawDataBase
from analyzer_helper.discord.utils.is_user_bot import UserBotChecker


class DiscordTransformRawData(TransformRawDataBase):
    def __init__(self, platform_id: str, guild_id: str):
        """
        Initializes the class with a specific platform ID and sets up a bot checker for user validation.
        """
        self.guild_id = guild_id
        self.platform_id = platform_id
        self.user_bot_checker = UserBotChecker(self.guild_id)

    def create_interaction_base(
        self,
        name: str,
        users_engaged_id: List[str],
        type: str,
    ) -> Dict[str, Any]:
        """
        Creates an interaction dictionary.

        Args:
            name (str): Name of the interaction (e.g., 'reply', 'mention').
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
            "date": data.get("createdDate"),
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

    def create_transformed_item(
        self, data: Dict[str, Any], interactions: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Creates a transformed item dictionary.

        Args:
            data (Dict[str, Any]): Raw data containing message details.
            interactions (List[Dict[str, Any]]): List of interaction dictionaries.

        Returns:
            Dict[str, Any]: Dictionary representing the transformed item.
        """
        is_bot = self.user_bot_checker.is_user_bot(data["author"])

        extracted_interactions = [
            interaction["interactions"] for interaction in interactions
        ]

        flat_interactions = [
            item for sublist in extracted_interactions for item in sublist
        ]

        return {
            "author_id": data["author"],
            "date": data.get("createdDate"),
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
        self,
        raw_data: list,
    ) -> List[Dict[str, Any]]:
        """
        Transforms raw Discord data into a structured format for analysis.

        This method processes a list of raw data dictionaries, extracting relevant
        interactions such as replies, mentions, and reactions, and organizes them
        into a list of transformed data dictionaries. Each dictionary represents
        an interaction or message, enriched with metadata and interaction details.

        Args:
            raw_data (list): A list of dictionaries containing raw Discord message data.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries representing the transformed data.
        """
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

                transformed_item = self.create_transformed_item(
                    data=data, interactions=interactions
                )
                transformed_data.append(transformed_item)
                # print(f"Added transformed item: {transformed_item}")
            except Exception as e:
                logging.error(
                    f"Error transforming raw discord data, "
                    f"messageId: {data['messageId']}. Error: {e}"
                )
        return transformed_data
