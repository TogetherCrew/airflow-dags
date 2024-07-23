from analyzer_helper.discourse.utils.convert_date_time_formats import DateTimeFormatConverter

class TransformRawInfo:

    def __init__(self):
        self.converter = DateTimeFormatConverter()
    
    def create_data_entry(self, raw_data: dict, interaction_type: str = None, interaction_user: int = None) -> dict:
        metadata = {
            "category_id": raw_data.get("category_id"),
            "topic_id": raw_data.get("topic_id"),
            "bot_activity": False,
        }

        result = {}
        if interaction_type == "reaction":
            result = {
                "actions": [],
                "author_id": str(interaction_user),
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
                    "users_engaged_id": [str(raw_data["author_id"] if interaction_type == "reply" else raw_data["replied_post_user_id"])],
                })

            result = {
                "author_id": str(interaction_user if interaction_type == "reply" else str(raw_data["author_id"])),
                "date": self.converter.convert_to_datetime(raw_data.get("created_at")),
                "source_id": str(raw_data["post_id"]),
                "metadata": metadata,           
                "actions": actions,
                "interactions": interactions,
            }
        
        return result

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
                    interaction_user=entry["replied_post_user_id"]
                ))
            # TODO: Create entry for mentioned users

        return transformed_data
        