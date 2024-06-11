from dags.hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class UserBotChecker:
    def __init__(self, platform_id):
        self.client = MongoSingleton.get_instance().client
        self.platform_id = platform_id
        self.db = self.client[self.platform_id]
        self.rawinfo_collection = self.db["rawinfos"]
        self.guildmembers_collection = self.db["guildmembers"]

    def is_user_bot(self, author_id):
        # Define the pipeline
        pipeline = [
            {"$match": {"author": author_id}},
            {
                "$lookup": {
                    "from": "guildmembers",
                    "localField": "author",
                    "foreignField": "discordId",
                    "as": "userDetails",
                }
            },
            {"$unwind": {"path": "$userDetails", "preserveNullAndEmptyArrays": True}},
            {"$project": {"isBot": "$userDetails.isBot"}},
        ]

        # Execute the aggregation and retrieve the first element
        result = list(self.rawinfo_collection.aggregate(pipeline))

        # Check if result exists and return isBot value (or default false)
        return result[0].get("isBot", False) if result else False
