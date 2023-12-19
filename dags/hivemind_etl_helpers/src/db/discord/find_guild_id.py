from bson import ObjectId
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


def find_guild_id_by_community_id(community_id: str) -> str:
    """
    find the guild id using the given community id

    Parameters
    ------------
    community_id : str
        the community id that the guild is for
    """

    client = MongoSingleton.get_instance().get_client()

    platform = client["Core"]["platforms"].find_one(
        {"community": ObjectId(community_id), "name": "discord"}, {"metadata.id": 1}
    )
    if platform is None:
        raise ValueError(
            f"The community id {community_id} within platforms does not exist!"
        )
    guild_id = platform["metadata"]["id"]
    return guild_id
