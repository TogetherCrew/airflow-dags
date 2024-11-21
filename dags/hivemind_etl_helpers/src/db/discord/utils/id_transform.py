from hivemind_etl_helpers.src.db.discord.utils.sort_based_id import sort_based_on_id
from tc_hivemind_backend.db.mongo import MongoSingleton


def convert_user_id(
    guild_id: str, ids: list[str]
) -> tuple[list[str], list[str], list[str]]:
    """
    convert a list of user id to their respective username

    Parameters
    -----------
    guild_id : str
        the guild id that user ids are in
    ids : list[str]
        a list of string each representing the user id

    Returns
    --------
    usernames : list[str]
        the list of user names
    global_names : list[str]
        the list of globalNames
    nicknames : list[str]
        the list of nicknames
    """
    client = MongoSingleton.get_instance().client

    cursor = client[guild_id]["guildmembers"].find(
        {"discordId": {"$in": (ids)}},
        {"username": 1, "globalName": 1, "nickname": 1, "discordId": 1, "_id": 0},
    )

    members_data = list(cursor)

    members_data_sorted = sort_based_on_id(ids, members_data, "discordId")

    usernames = [member["username"] for member in members_data_sorted]
    global_names = [member["globalName"] for member in members_data_sorted]
    nicknames = [member["nickname"] for member in members_data_sorted]
    return usernames, global_names, nicknames


def convert_role_id(guild_id: str, ids: list[str]) -> list[str]:
    """
    convert a list of role id to a list with their respective role name

    Parameters
    -----------
    guild_id : str
        the guild id that user ids are in
    ids : list[str]
        a list of string each representing the role id

    Returns
    --------
    usernames : list[str]
        the list of user names
    """
    client = MongoSingleton.get_instance().client

    cursor = client[guild_id]["roles"].find(
        {"roleId": {"$in": ids}}, {"name": 1, "roleId": 1, "_id": 0}
    )

    members_data = list(cursor)

    members_data_sorted = sort_based_on_id(ids, members_data, "roleId")

    usernames = [member["name"] for member in members_data_sorted]

    return usernames
