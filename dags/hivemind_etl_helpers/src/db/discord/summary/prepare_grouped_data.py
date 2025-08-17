from datetime import datetime

from hivemind_etl_helpers.src.db.discord.utils.fetch_channel_thread_name import (
    FetchDiscordChannelThreadNames,
)
from hivemind_etl_helpers.src.db.discord.fetch_raw_messages import fetch_raw_msg_grouped
from dateutil.parser import parse


def prepare_grouped_data(
    guild_id: str,
    from_date: datetime,
    selected_channels: list[str],
) -> dict[float | str, dict[str, dict[str | None, list]]]:
    """
    prepare the nested dictionary of grouped data

    Parameters
    ------------
    guild_id : str
        the guild id to prepare its llama_index documents
    from_date : datetime
        get the raw data from a specific date
        default is None, meaning get all the messages
    selected_channels : list[str]
        the channel ids selected to be processed

    Returns
    --------
    raw_data_grouped : dict[float | str, dict[str, dict[str | None, list]]]
        grouping the messages into a nested dictionary
        first level should be representative of day, second level channel
        and third level would be the thread
    """
    raw_daily_grouped = {}
    raw_mongo_grouped_messages = fetch_raw_msg_grouped(
        guild_id, from_date, selected_channels
    )

    # saving the grouping per day
    for msg in raw_mongo_grouped_messages:
        msg_date = parse(msg["_id"]["date"]).timestamp()
        raw_daily_grouped[msg_date] = msg["messages"]

    raw_data_grouped = group_per_channel_thread(guild_id=guild_id, daily_messages=raw_daily_grouped)

    return raw_data_grouped


def group_per_channel_thread(
    guild_id: str,
    daily_messages: dict[float, list]
) -> dict[float | str, dict[str, dict[str | None, list]]]:
    """
    group the data into a nested dictionary.
    Note that the daily_messages should be already grouped by day

    Parameters
    ------------
    guild_id : str
        the guild id to access data
    daily_messages : dict[float, list]
        a dictionary that each key represents the day timestamp
        and values are a list of messages in that day

    Returns
    ---------
    raw_data_grouped : dict[float | str, dict[str, dict[str | None, list]]]
        grouping the messages into a nested dictionary
        first level should be representative of day, second level channel
        and third level would be the thread
        (thread can be `None` meaning it is the main channel)
    """
    raw_data_grouped: dict[float | str, dict[str, dict[str | None, list]]] = {}

    name_fetcher = FetchDiscordChannelThreadNames(guild_id)

    # grouping by channel
    for date in daily_messages.keys():
        for msg in daily_messages[date]:
            channel = name_fetcher.fetch_discord_channel_name(msg["channelId"])
            thread = name_fetcher.fetch_discord_thread_name(msg["threadId"]) if msg["threadId"] else None

            raw_data_grouped.setdefault(date, {}).setdefault(channel, {}).setdefault(
                thread, []
            ).append(msg)

    return raw_data_grouped
