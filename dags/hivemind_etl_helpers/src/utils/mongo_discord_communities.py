from hivemind_etl_helpers.src.utils.get_communities_data import query_modules_db


def get_all_discord_communities() -> list[str]:
    """
    get discord communities that hivemind is enabled for them

    Returns
    ---------
    community_ids : list[str]
        a list of community id that were set in modules

    """
    community_ids: list[str] = []
    discourse_modules = query_modules_db(platform="discord")
    for module in discourse_modules:
        community_id = str(module["communityId"])
        community_ids.append(community_id)

    return community_ids
