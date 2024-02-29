from datetime import datetime

from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


def get_github_communities_and_orgs() -> list[dict[str, str | datetime]]:
    """
    get all the github communities and the
    related github organization ids

    Returns
    ---------
    community_orgs : list[dict[str, str | datetime]]
        keys are the community id
        and values are the related organizations connected for that community
        the first item in the list of values is the github organization id
        the second item in list of values is the fromDate

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
    github_modules = query_github_modules_db()
    for module in github_modules:
        community_id = str(module["communityId"])

        options = module["options"]
        for platform in options:
            # this is happening because of the unwind operator
            platform_data = platform["platforms"]

            platform_from_date = platform_data["fromDate"]
            organization_id = platform_data["organizationId"]

            community_orgs.append(
                {
                    "community_id": community_id,
                    "from_date": platform_from_date,
                    "organization_id": organization_id,
                }
            )

    return community_orgs


def query_github_modules_db() -> list[dict]:
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
        {"$match": {"platform_data.name": "github"}},
        {
            "$addFields": {
                "options.platforms.organizationId": "$platform_data.metadata.organizationId"
            }
        },
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
