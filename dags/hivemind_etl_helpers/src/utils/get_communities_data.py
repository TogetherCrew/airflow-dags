"""
This file to be removed in future and we would move the features to directory
/dags/hivemind_etl_helpers/src/utils/modules/
"""

from datetime import datetime

from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


def query_modules_db(platform: str) -> list[dict]:
    """
    query the modules database for to get platforms' metadata

    Parameters
    -----------
    platform : str
        the platform to choose
        it can be `github`, `discourse`, `discord` or etc

    """
    client = MongoSingleton.get_instance().client

    pipeline = [
        {"$match": {"name": "hivemind"}},
        {"$unwind": "$options.platforms"},
        {
            "$lookup": {
                "from": "platforms",
                "localField": "options.platforms.platformId",
                "foreignField": "_id",
                "as": "platform_data",
            }
        },
        {"$unwind": "$platform_data"},
        {"$match": {"platform_data.name": platform}},
        {"$addFields": {"options.platforms.metadata": "$platform_data.metadata"}},
        {
            "$group": {
                "_id": "$_id",
                "name": {"$first": "$name"},
                "communityId": {"$first": "$communityId"},
                "options": {"$push": "$options"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "name": 0,
            }
        },
    ]
    cursor = client["Core"]["modules"].aggregate(pipeline)

    return list(cursor)


def get_google_drive_communities() -> (
    list[dict[str, str | list[str] | datetime | dict]]
):
    """
    Get Google Drive communities with their folder IDs, file IDs, drive IDs, and client config.

    Returns
    ---------
    communities_data : list[dict[str, str| list[str] | datetime | dict]]
        a list of Google Drive data information

        example data output:
        ```
        [{
          "community_id": "community1",
          "from_date": datetime(2024, 1, 1),
          "folder_ids": ["folder_123"],
          "file_ids": ["file_abc"],
          "drive_ids": ["drive_xyz"],
          "client_config": {...}
        }]
        ```
    """
    communities_data: list[dict[str, str | list[str] | datetime | dict]] = []
    google_drive_modules = query_modules_db(platform="google-drive")

    for module in google_drive_modules:
        community_id = str(module["communityId"])
        options = module["options"]

        for platform in options:
            platform_data = platform["platforms"]
            platform_from_date = platform_data["fromDate"]
            folder_ids = platform_data["metadata"]["folder_ids"]
            file_ids = platform_data["metadata"]["file_ids"]
            drive_ids = platform_data["metadata"]["drive_ids"]
            client_config = platform_data["metadata"]["client_config"]

            communities_data.append(
                {
                    "community_id": community_id,
                    "from_date": platform_from_date,
                    "folder_ids": folder_ids,
                    "file_ids": file_ids,
                    "drive_ids": drive_ids,
                    "client_config": client_config,
                }
            )

    return communities_data


def get_all_notion_communities() -> list[dict[str, str | list[str] | datetime | dict]]:
    """
    Get all the Notion communities with their database IDs, page IDs, and client config.

    Returns
    ---------
    community_orgs : list[dict[str, str | list[str] | datetime | dict]] = []
        a list of Notion data information

        example data output:
        ```
        [{
          "community_id": "6579c364f1120850414e0dc5",
          "from_date": datetime(2024, 1, 1),
          "database_ids": ["dadd27f1dc1e4fa6b5b9dea76858dabe"],
          "page_ids": ["6a3c20b6861145b29030292120aa03e6"],
          "client_config": {...}
        }]
        ```
    """
    communities_data: list[dict[str, str | list[str] | datetime | dict]] = []
    notion_modules = query_modules_db(platform="notion")
    for module in notion_modules:
        community_id = str(module["communityId"])

        options = module["options"]
        for platform in options:
            # this is happening because of the unwind operator
            platform_data = platform["platforms"]
            platform_from_date = platform_data["fromDate"]
            database_ids = platform_data["metadata"]["database_ids"]
            page_ids = platform_data["metadata"]["page_ids"]
            client_config = platform_data["metadata"]["client_config"]

            communities_data.append(
                {
                    "community_id": community_id,
                    "from_date": platform_from_date,
                    "database_ids": database_ids,
                    "page_ids": page_ids,
                    "client_config": client_config,
                }
            )
    return communities_data
