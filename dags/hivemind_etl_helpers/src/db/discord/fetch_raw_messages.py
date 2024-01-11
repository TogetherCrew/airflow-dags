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

    raw_messages: list[dict]
    if from_date is not None:
        cursor = (
            client[guild_id]["rawinfos"]
            .find(
                {
                    "createdDate": {"$gte": from_date},
                    "isGeneratedByWebhook": False,
                }
            )
            .sort("createdDate", 1)
        )
        raw_messages = list(cursor)
    else:
        cursor = (
            client[guild_id]["rawinfos"]
            .find({"isGeneratedByWebhook": False})
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

    # the pipeline to apply through mongodb
    pipeline: list[dict] = []

    if from_date is not None:
        pipeline.append(
            {
                "$match": {
                    "$and": [
                        {
                            "createdDate": {
                                "$gte": from_date,
                                "$lt": datetime.now().replace(
                                    hour=0, minute=0, second=0, microsecond=0
                                ),
                            }
                        },
                        {"isGeneratedByWebhook": False},
                    ]
                }
            },
        )
    else:
        pipeline.append(
            {
                "$match": {
                    "isGeneratedByWebhook": False,
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

def fetch_channels(guild_id: str):
    """
    fetch the channels from modules that we wanted to process

    Parameters
    -----------
    guild_id : str
        the guild to have its channels

    Returns
    ---------
    channels : list[str]
        the channels to fetch data from
    """
    pass