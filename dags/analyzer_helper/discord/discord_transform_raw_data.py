from datetime import datetime

from dags.analyzer_helper.discord.transform_raw_data_base import TransformRawDataBase


class DiscordTransformRawData(TransformRawDataBase):
    def transform(self, raw_data: list, platform_id: str, period: datetime) -> list[dict]:
        transformed_data = []
        for data in raw_data:
            interactions = []
            if data.get("replied"):
                interactions.append({
                    "name": "reply",
                    "users_engaged_id": [data.get("replied")],
                    "type": "receiver"
                })

            if data.get("user_mentions"):
                interactions.append({
                    "name": "mention",
                    "users_engaged_id": data.get("user_mentions"),
                    "type": "emitter"
                })

            if data.get("reactions"):
                for reaction in data.get("reactions"):
                    parts = reaction.split(", ")
                    if len(parts) > 2:
                        interactions.append({
                            "name": "reaction",
                            "users_engaged_id": parts[:-1],
                            "type": "emitter"
                        })

            action_name = "message"
            action_type = "emitter" if not data.get("isGeneratedByWebhook", False) else "webhook"

            transformed_item = {
                "author_id": data.get("author", ""),
                "date": period,
                "source_id": platform_id,
                "metadata": {
                    "channel_id": data.get("channelId", ""),
                    "channel_name": data.get("channelName", ""),
                    "thread_id": data.get("threadId", ""),
                    "thread_name": data.get("threadName", ""),
                    "message_id": data.get("messageId", ""),
                    "is_generated_by_webhook": data.get("isGeneratedByWebhook", False)
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
        return transformed_data
