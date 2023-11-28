def remove_nested_collections(data):
    """
    Removes any nested dictionaries or lists from the input dictionary.
    
    Args:
    data (dict): A dictionary potentially containing nested dictionaries or lists.
    
    Returns:
    dict: A dictionary with nested dictionaries and lists removed.
    """
    if not isinstance(data, dict):
        raise ValueError("Input must be a dictionary")

    # Iterate over a copy of the items to avoid runtime error due to changing dict size
    for key, value in list(data.items()):
        if isinstance(value, (dict, list)):
            del data[key]
    
    return data