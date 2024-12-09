from datetime import datetime

from hivemind_etl_helpers.src.db.discord.fetch_raw_messages import fetch_raw_messages
from hivemind_etl_helpers.src.db.discord.utils.transform_discord_raw_messges import (
    transform_discord_raw_messages,
)
from llama_index.core import Document
from hivemind_etl_helpers.src.db.discord.preprocessor import DiscordPreprocessor


def discord_raw_to_documents(
    guild_id: str,
    selected_channels: list[str],
    from_date: datetime,
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
    raw_mongo_messages = fetch_raw_messages(guild_id, selected_channels, from_date)
    processed_messages = update_raw_messages(raw_data=raw_mongo_messages)
    messages_docuemnt = transform_discord_raw_messages(guild_id, processed_messages)

    return messages_docuemnt


def update_raw_messages(raw_data: list[dict]) -> list[dict]:
    """
    Update raw messages text by cleaning their data

    Parameters
    -----------
    data : list[dict]
        a list of raw data fetched from database
        each dict hold a 'content'

    Returns
    ---------
    cleaned_data : list[dict]
        a list of dictionaries but with cleaned data
    """
    preprocessor = DiscordPreprocessor()

    cleaned_data: list[dict] = []
    for data in raw_data:
        content = data.get("content")
        if content:
            cleaned_content = preprocessor.clean_text(content)

            if cleaned_content:
                data["content"] = cleaned_content
                cleaned_data.append(data)

    return cleaned_data
