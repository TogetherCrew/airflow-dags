def sort_based_on_id(
    ids: list[str], data: list[dict[str, dict]], id_key: str
) -> list[dict[str, dict]]:
    """
    sort the data based on id

    Parameters
    -----------
    ids : list[str]
        a list of id representing multiple entities
    data : list[dict[str, dict]]
        the list of data that contains the id and the names for the ids
    id_key : str
        the id field name for the entity
        for example, for members it would be `discordId`

    Returns
    ---------
    sorted_data : list[dict[str, dict]]
        the data sorted based on id
    """
    id_dict = {d[id_key]: d for d in data}
    sorted_data = [id_dict[id] for id in ids if id in id_dict]

    return sorted_data
