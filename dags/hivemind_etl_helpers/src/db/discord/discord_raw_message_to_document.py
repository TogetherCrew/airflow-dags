from datetime import datetime

from llama_index import Document

from hivemind_etl_helpers.src.db.discord.fetch_raw_messages import fetch_raw_messages
from hivemind_etl_helpers.src.db.discord.utils.transform_discord_raw_messges import (
    transform_discord_raw_messages,
)


def discord_raw_to_docuemnts(
    guild_id: str, from_date: datetime | None = None
) -> list[Document]:
    """
    fetch the discord raw messages from db and convert them to llama_index Documents

    Parameters
    -----------
    guild_id : str
        the guild id to fetch their `rawinfos` messages
    from_date : datetime | None
        get the raw data from a specific date
        default is None, meaning get all the messages

    Returns
    ---------
    messages_docuemnt : list[llama_index.Document]
        list of messages converted to documents
    """

    raw_mongo_messages = fetch_raw_messages(guild_id, from_date)
    messages_docuemnt = transform_discord_raw_messages(guild_id, raw_mongo_messages)

    return messages_docuemnt