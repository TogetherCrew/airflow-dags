from datetime import datetime
from analyzer_helper.discord.transform_raw_data_base import TransformRawDataBase
import logging


class DiscordTransformRawData(TransformRawDataBase):
    def transform(self, raw_data: list, platform_id: str, period: datetime) -> list[dict]:
        transformed_data = []
        for data in raw_data:
            try:
                if "author" not in data or not data["author"]:
                    raise ValueError("Missing 'author' in raw data")

                interactions = []

                if data.get("replied_user"):
                    interactions.append({
                        "name": "reply",
                        "users_engaged_id": [data["replied_user"]],
                        "type": "emitter"
                    })
                    # Create another document for the receiver
                    receiver_interaction = {
                        "author_id": data["replied_user"],
                        "date": period,
                        "source_id": data["messageId"],
                        "metadata": {
                            "thread_id": data["threadId"],
                            "channel_id": data["channelId"],
                            "bot_activity": data["isGeneratedByWebhook"] or data["botActivity"],
                            # "channel_name": data["channelName"],
                            # "thread_name": data["threadName"],
                        },
                        "actions": [
                            {
                                "name": "reply",
                                "type": "receiver"
                            }
                        ],
                        "interactions": []
                    }
                    transformed_data.append(receiver_interaction)

                if data.get("user_mentions"):
                    interactions.append({
                        "name": "mention",
                        "users_engaged_id": data["user_mentions"],
                        "type": "emitter"
                    })

                all_reaction_users = []
                if data.get("reactions"):
                    for reaction in data["reactions"]:
                        parts = reaction.split(",")
                        if len(parts) > 2:
                            all_reaction_users.extend(parts[:-1])

                    if all_reaction_users:
                        interactions.append({
                            "name": "reaction",
                            "users_engaged_id": all_reaction_users,
                            "type": "receiver"
                        })

                    # Create a document for each user in reactions with type emitter
                    for user in all_reaction_users:
                        emitter_interaction = {
                            "author_id": user,
                            "date": period,
                            "source_id": data["messageId"],
                            "metadata": {
                                "thread_id": data["threadId"],
                                "channel_id": data["channelId"],
                                "bot_activity": data["isGeneratedByWebhook"] or data["botActivity"],
                                # "channel_name": data["channelName"],
                                # "thread_name": data["threadName"],
                            },
                            "actions": [
                                {
                                    "name": "reaction",
                                    "type": "emitter"
                                }
                            ],
                            "interactions": []
                        }
                        transformed_data.append(emitter_interaction)

                action_name = "message"
                action_type = "emitter"

                transformed_item = {
                    "author_id": data["author"],
                    "date": period,
                    "source_id": data["messageId"],
                    "metadata": {
                            "thread_id": data["threadId"],
                            "channel_id": data["channelId"],
                            "bot_activity": data["isGeneratedByWebhook"] or data["botActivity"],
                            # "channel_name": data["channelName"],
                            # "thread_name": data["threadName"],
                    },
                    "actions": [
                        {
                            "name": action_name,
                            "type": action_type
                        }
                    ],
                    "interactions": interactions
                }
                transformed_data.append(transformed_item)
            except Exception as e:
                logging.error(f"Error transforming raw discord data. Error: {e}")

        return transformed_data
