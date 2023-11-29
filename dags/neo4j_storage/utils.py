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

def flat_map(obj):
    """
    Function to iterate over an object and flat map its values if the value is an object or an array.

    The function works recursively, handling nested dictionaries and lists. For each nested
    element, it creates a flattened key based on the path to that element.

    Parameters:
    obj (dict or list): The object to be flattened.

    Returns:
    dict: A dictionary with flattened keys and values.

    Example:
    Given an object like:
    ```
    {
        "name": "John",
        "address": {
            "street": "Main St",
            "city": "Springfield"
        },
        "phones": ["123-456-7890", "987-654-3210"]
    }
    ```

    The function will return:
    ```
    {
        'name': 'John',
        'address.street': 'Main St',
        'address.city': 'Springfield',
        'phones.0': '123-456-7890',
        'phones.1': '987-654-3210'
    }
    ```
    """
    if isinstance(obj, dict):
        # For dictionaries, iterate through each key-value pair
        flat = {}
        for key, value in obj.items():
            # Recursively flat map the value if it's a dictionary or list
            if isinstance(value, (dict, list)):
                for inner_key, inner_value in flat_map(value).items():
                    # Combine the outer key with the inner key
                    flat[f"{key}.{inner_key}"] = inner_value
            else:
                flat[key] = value
        return flat
    elif isinstance(obj, list):
        # For lists, iterate through each element
        flat = {}
        for index, value in enumerate(obj):
            # Recursively flat map the value if it's a dictionary or list
            if isinstance(value, (dict, list)):
                for inner_key, inner_value in flat_map(value).items():
                    # Use the index as the key
                    flat[f"{index}.{inner_key}"] = inner_value
            else:
                flat[index] = value
        return flat
    else:
        # If the object is neither a dictionary nor a list, return it as is
        return {0: obj}
