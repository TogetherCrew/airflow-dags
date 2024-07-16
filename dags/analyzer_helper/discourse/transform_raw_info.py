from analyzer_helper.discourse.utils.convert_date_time_formats import DateTimeFormatConverter

class TransformRawInfo:

    def __init__(self):
        self.converter = DateTimeFormatConverter()
    
    def create_data_entry(self, raw_data: list, interaction_type: str = None, interaction_user: int = None):
        metadata = {
            "channel_id": raw_data.get("category_id"),
            "thread_id": raw_data.get("topic_id"),
            "bot_activity": False,
        }

        if interaction_type == "reaction":
            return {
                "actions": [],
                "author_id": interaction_user,
                "date": self.converter.convert_to_datetime(raw_data.get("created_at")),
                "interactions": [
                    {
                        "name": "reaction",
                        "type": "emitter",  
                        "users_engaged_id": [str(raw_data["post_id"])],
                    }
                ],
                "metadata": metadata,
                "source_id": str(raw_data["post_id"]),
            }
        else:
            actions = [{"name": "message", "type": "emitter"}] if not interaction_type else []
            interactions = []
            if raw_data["reactions"]:
                interactions.append({
                    "name": "reaction",
                    "type": "receiver",
                    "users_engaged_id": [str(int(reaction)) for reaction in raw_data["reactions"]],
                })
            if raw_data["replied_post_id"]:
                interactions.append({
                    "name": "reply",
                    "type": "emitter" if interaction_type != "reply" else "receiver",
                    "users_engaged_id": [str(raw_data["author_id"] if interaction_type == "reply" else raw_data["replied_post_id"])],
                })

            return {
                "author_id": str(interaction_user if interaction_type == "reply" else raw_data["author_id"]),
                "date": self.converter.convert_to_datetime(raw_data.get("created_at")),
                "source_id": str(raw_data["post_id"]),
                "metadata": metadata,           
                "actions": actions,
                "interactions": interactions,
            }

    def transform(self, raw_data: list) -> list:
        transformed_data = []
        for entry in raw_data:
            # Create main post entry
            transformed_data.append(self.create_data_entry(entry))

            # Create entries for reactions
            for reaction in entry["reactions"]:
                transformed_data.append(self.create_data_entry(
                    entry,
                    interaction_type="reaction",
                    interaction_user=int(reaction)
                ))

            # Create entry for reply
            if entry["replied_post_id"]:
                transformed_data.append(self.create_data_entry(
                    entry,
                    interaction_type="reply",
                    interaction_user=entry["replied_post_id"]
                ))
            # TODO: Create entry for mentioned users

        return transformed_data

# Sample data
data = [
    {"post_id": 6262, "author_id": 6168, "created_at" : "2023-09-11T21:41:43.553Z", "author_name": "Ibby Benali", "reactions": [], "replied_post_id": 6512, "topic_id": 6134},
    {"post_id": 6261, "author_id": 6168, "created_at" : "2023-09-11T21:42:43.553Z", "author_name": "Ibby Benali", "reactions": [1, 2], "replied_post_id": None, "topic_id": 6134},
]

# Create an instance of the class with the raw data
transformer = TransformRawInfo()

# Transform the data
transformed_data = transformer.transform(data)
# import json
# print(json.dumps(transformed_data, indent=2))
print(transformed_data)