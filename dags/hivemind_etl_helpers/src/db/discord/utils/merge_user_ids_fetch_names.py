from hivemind_etl_helpers.src.db.discord.utils.id_transform import convert_user_id


def merge_user_ids_and_fetch_names(
    guild_id: str, *args: list[list[str]]
) -> tuple[list[str], list[str], list[str]]:
    """
    get multiple list of user ids and fetch the related names for each.
    Using this function can fewer the database calls.

    Parameters
    -----------
    guild_id : str
        the guild id to fetch the users from
    args : list[list[str]]
        should be a list of user id lists

    Returns
    --------
    user_names : list[list[str]]
        list of list of usernames
        each second list is related to a list of user ids within args
    global_names : list[list[str]]
        list of list of globalNames
        each second list is related to a list of user ids within args
    nicknames : list[list[str]]
        list of list of nicknames
        each second list is related to a list of user ids within args
    """
    user_ids = []
    for ids in args:
        user_ids.extend(ids)
    num_arrays = len(args)
    split_indices = [len(ids) for ids in args]

    user_names, global_names, nicknames = convert_user_id(guild_id, user_ids)

    result_usernames = []
    result_global_names = []
    result_nicknames = []
    start_idx = 0
    for i in range(num_arrays):
        end_idx = start_idx + split_indices[i]
        result_usernames.append(user_names[start_idx:end_idx])
        result_global_names.append(global_names[start_idx:end_idx])
        result_nicknames.append(nicknames[start_idx:end_idx])

        start_idx = end_idx

    return result_usernames, result_global_names, result_nicknames
