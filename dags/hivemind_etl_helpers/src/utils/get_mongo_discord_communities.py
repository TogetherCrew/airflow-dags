from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


def get_all_discord_communities() -> list[str]:
    """
    Getting all communities having discord from database for hivemind ETL
    """
    mongo = MongoSingleton.get_instance()
    cursor = mongo.client["Module"]["modules"].find(
        {
            "name": "hivemind",
        },
        {"communityId": 1, "_id": 0},
    )
    results = list(cursor)

    # getting the community id
    communities = [str(hivemind_module["communityId"]) for hivemind_module in results]
    return communities
