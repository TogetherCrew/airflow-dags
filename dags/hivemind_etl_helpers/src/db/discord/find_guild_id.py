from bson import ObjectId
from tc_hivemind_backend.db.mongo import MongoSingleton


def find_guild_id_by_platform_id(platform_id: str) -> str:
    """
    find the guild id using the given platform id

    Parameters
    ------------
    platform_id : str
        the community id that the guild is for
    """

    client = MongoSingleton.get_instance().get_client()

    platform = client["Core"]["platforms"].find_one(
        {"_id": ObjectId(platform_id), "name": "discord"}, {"metadata.id": 1}
    )
    if platform is None:
        raise ValueError(
            f"The community id {platform_id} within platforms does not exist!"
        )
    guild_id = platform["metadata"]["id"]
    return guild_id
