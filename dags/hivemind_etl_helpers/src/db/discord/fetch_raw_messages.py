from datetime import datetime

from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


def fetch_raw_messages(guild_id: str, from_date: datetime | None = None) -> list[dict]:
    """
    fetch rawinfo messages from mongodb database

    Parameters
    -----------
    guild_id : str
        the guild id to fetch their `rawinfos` messages
    from_date : datetime
        get the raw data from a specific date
        default is None, meaning get all the messages

    Returns
    --------
    raw_messages : list[dict]
        a list of raw messages
    """
    client = MongoSingleton.get_instance().get_client()

    channels, from_date_modules = fetch_channels_and_from_date(guild_id=guild_id)

    raw_messages: list[dict]
    if from_date is not None:
        cursor = (
            client[guild_id]["rawinfos"]
            .find(
                {
                    "createdDate": {"$gte": from_date},
                    "isGeneratedByWebhook": False,
                    "channelId": {"$in": channels},
                }
            )
            .sort("createdDate", 1)
        )
        raw_messages = list(cursor)
    else:
        cursor = (
            client[guild_id]["rawinfos"]
            .find(
                {
                    "isGeneratedByWebhook": False,
                    "channelId": {"$in": channels},
                    "createdDate": {"$gte": from_date_modules},
                }
            )
            .sort("createdDate", 1)
        )
        raw_messages = list(cursor)

    return raw_messages


def fetch_raw_msg_grouped(
    guild_id: str, from_date: datetime | None = None, sort: int = 1
) -> list[dict[str, dict]]:
    """
    fetch raw messages grouped by day
    this would fetch the data until 1 day ago

    Parameters
    -----------
    guild_id : str
        the guild id to fetch their `rawinfos` messages
    from_date : datetime
        get the raw data from a specific date
        default is None, meaning get all the messages
    sort : int
        sort the data Ascending or Descending
        `1` represents for Ascending
        `-1` represents for Descending

    Returns
    --------
    raw_messages_grouped : list[dict[str, list]]
        ascending sorted list of raw messages
        it would be a list, each having something like below
        the date is in format of `%Y-%m-%d`
        ```
                "_id": {
                    "date": str
                },
                "messages": dict[str, Any],
        ```
    """
    client = MongoSingleton.get_instance().client

    channels, from_date_modules = fetch_channels_and_from_date(guild_id=guild_id)

    # the pipeline to apply through mongodb
    pipeline: list[dict] = []

    if from_date is not None:
        pipeline.append(
            {
                "$match": {
                    "createdDate": {
                        "$gte": from_date,
                        "$lt": datetime.now().replace(
                            hour=0, minute=0, second=0, microsecond=0
                        ),
                    },
                    "isGeneratedByWebhook": False,
                    "channelId": {"$in": channels},
                }
            },
        )
    else:
        pipeline.append(
            {
                "$match": {
                    "createdDate": {
                        "$gte": from_date_modules,
                        "$lt": datetime.now().replace(
                            hour=0, minute=0, second=0, microsecond=0
                        ),
                    },
                    "isGeneratedByWebhook": False,
                    "channelId": {"$in": channels},
                }
            },
        )

    # sorting
    pipeline.append(
        {"$sort": {"createdDate": sort}},
    )

    # add the grouping
    pipeline.append(
        {
            "$group": {
                "_id": {
                    "date": {
                        "$dateToString": {"format": "%Y-%m-%d", "date": "$createdDate"}
                    },
                },
                "messages": {"$push": "$$ROOT"},
            }
        }
    )

    cursor = client[guild_id]["rawinfos"].aggregate(pipeline)
    raw_messages_grouped = list(cursor)

    return raw_messages_grouped


def fetch_channels_and_from_date(guild_id: str) -> tuple[list[str], datetime | None]:
    """
    fetch the channels and the `fromDate` to process
    from Module that we wanted to process

    Parameters
    -----------
    guild_id : str
        the guild to have its channels

    Returns
    ---------
    channels : list[str]
        the channels to fetch data from
    from_date : datetime | None
        the processing from_date
    """
    client = MongoSingleton.get_instance().client
    platform = client["Core"]["platforms"].find_one(
        {"name": "discord", "metadata.id": guild_id},
        {
            "_id": 1,
            "community": 1,
        },
    )

    if platform is None:
        raise ValueError(f"No platform with given guild_id: {guild_id} available!")

    result = client["Module"]["modules"].find_one(
        {
            "communityId": platform["community"],
            "options.platforms.platformId": platform["_id"],
        },
        {"_id": 0, "options.platforms.$": 1},
    )

    channels: list[str]
    from_date: datetime | None = None
    if result is not None:
        channels = result["options"]["platforms"][0]["options"]["channels"]
        from_date = result["options"]["platforms"][0]["fromDate"]
    else:
        raise ValueError("No modules set for this community!")

    return channels, from_date
