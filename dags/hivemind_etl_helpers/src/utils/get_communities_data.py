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


def get_github_communities_data() -> list[dict[str, str | datetime]]:
    """
    get all the github communities and the
    related github organization ids

    Returns
    ---------
    community_orgs : list[dict[str, str | datetime]]
        a list of github community information

        example data output:
        ```
        [{
            "community_id": "community1",
            "organization_id": "organization1",
            "from_date": datetime(2024, 1, 1)
        }]
        ```
    """
    community_orgs: list[dict[str, str | datetime]] = []
    github_modules = query_modules_db(platform="github")
    for module in github_modules:
        community_id = str(module["communityId"])

        options = module["options"]
        for platform in options:
            # this is happening because of the unwind operator
            platform_data = platform["platforms"]

            platform_from_date = platform_data["fromDate"]
            organization_id = platform_data["metadata"]["organizationId"]

            community_orgs.append(
                {
                    "community_id": community_id,
                    "from_date": platform_from_date,
                    "organization_id": organization_id,
                }
            )

    return community_orgs


def get_discourse_communities() -> list[dict[str, str | datetime]]:
    """
    get discourse communities with their forum endpoint


    Returns
    ---------
    communities_data : list[dict[str, str | datetime]]
        a list of discourse data information

        example data output:
        ```
        [{
            "community_id": "community1",
            "endpoint": "forum.endpoint.com",
            "from_date": datetime(2024, 1, 1)
        }]
        ```
    """
    communities_data: list[dict[str, str | datetime]] = []
    discourse_modules = query_modules_db(platform="discourse")
    for module in discourse_modules:
        community_id = str(module["communityId"])

        options = module["options"]
        for platform in options:
            # this is happening because of the unwind operator
            platform_data = platform["platforms"]

            platform_from_date = platform_data["fromDate"]
            organization_id = platform_data["metadata"]["endpoint"]

            communities_data.append(
                {
                    "community_id": community_id,
                    "from_date": platform_from_date,
                    "endpoint": organization_id,
                }
            )

    return communities_data


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
          "folder_id": "folder_123",
          "file_id": "file_abc",
          "drive_id": "drive_xyz",
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
            folder_id = platform_data["metadata"]["folder_id"]
            file_id = platform_data["metadata"]["file_id"]
            drive_id = platform_data["metadata"]["drive_id"]
            client_config = platform_data["metadata"]["client_config"]

            communities_data.append(
                {
                    "community_id": community_id,
                    "from_date": platform_from_date,
                    "folder_id": folder_id,
                    "file_id": file_id,
                    "drive_id": drive_id,
                    "client_config": client_config,
                }
            )

    return communities_data
