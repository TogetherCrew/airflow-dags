from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


def get_all_discord_communities() -> list[str]:
    """
    Getting all communities having discord from database
    """
    mongo = MongoSingleton.get_instance()
    communities = (
        mongo.client["Core"]["platforms"]
        .find({"name": "discord"})
        .distinct("community")
    )
    # getting the str instead of ObjectId
    communities = [str(comm) for comm in communities]
    return communities
