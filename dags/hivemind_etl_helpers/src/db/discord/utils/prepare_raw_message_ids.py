def prepare_raw_message_ids(
    message: str, roles: dict[str, str], users: dict[str, str]
) -> str:
    """
    convert the ids within the message to their respective name

    Parameters
    ------------
    message : str
        the raw message that can contain
    roles : dict[str, str]
        the roles id and name within dictionary
        ids are keys and role name are values
    users : dict[str, str]
        the user id and name within dictionary
        ids are keys and username are values


    Returns
    --------
    updated_message : str
        the message that all the ids are updated to names
    """
    # for initializing
    updated_message = message

    for role_id in roles.keys():
        # the role within the message
        role_in_message = f"<@{role_id}>"
        updated_message = updated_message.replace(role_in_message, roles[role_id])

    for user_id in users.keys():
        # the user id within the message
        user_in_message = f"<@{user_id}>"
        updated_message = updated_message.replace(user_in_message, users[user_id])

    return updated_message
