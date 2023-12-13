def prepare_raction_ids(reactions: list[str]) -> list[str]:
    """
    fetch the user ids from reactions of a message.
    for example the reactions are as below
    - ["userId1, userId2,:thumbsup:", "userId2, userId4,:heart:"]

    and we have to extract the just the userids which the return of the example would be
    - ["userId1", "userId2", "userId4"]

    Parameters
    ------------
    reactions : list[str]
        a list of reactions with different emojis

    Returns
    ---------
    user_ids : list[str]
        the list of users reacting to a message
    """
    user_ids = []

    for reaction in reactions:
        parts = reaction.split(",")
        # Append all user ids except the last part (emoji)
        user_ids.extend(parts[:-1])

    # making unique
    user_ids = list(set(user_ids))
    return user_ids
