import logging
import re
from typing import Any

from hivemind_etl_helpers.src.db.discord.utils.fetch_channel_thread_name import (
    FetchDiscordChannelThreadNames,
)
from hivemind_etl_helpers.src.db.discord.utils.content_parser import (
    remove_empty_str,
    remove_none_from_list,
)
from hivemind_etl_helpers.src.db.discord.utils.id_transform import convert_role_id
from hivemind_etl_helpers.src.db.discord.utils.merge_user_ids_fetch_names import (
    merge_user_ids_and_fetch_names,
)
from hivemind_etl_helpers.src.db.discord.utils.prepare_raw_message_ids import (
    prepare_raw_message_ids,
)
from hivemind_etl_helpers.src.db.discord.utils.prepare_reactions_id import (
    prepare_raction_ids,
)
from hivemind_etl_helpers.src.db.globals import DATE_FORMAT
from llama_index.core import Document
from tc_hivemind_backend.db.utils.preprocess_text import BasePreprocessor


def transform_discord_raw_messages(
    guild_id: str,
    messages: list[dict],
    exclude_metadata: bool = False,
) -> list[Document]:
    """
    transform the raw messages of discord to llama_index docuemnts
    1. Extract meta-data of message
    2. convert messages to documents and add the meta-data for each

    Paramaters
    ------------
    guild_id : str
        the discord guild that messages are related to
    messages : list[dict]
        list of raw messages with their respective fields
    exclude_metadata : bool
        whether to have all metadata or have nothing
        default is false meaning not to exclude any metadata

    Returns
    ---------
    messages_docuemnt : list[llama_index.Document]
        list of messages converted to documents
    """
    documents = []

    for msg in messages:
        try:
            doc = prepare_document(
                message=msg, guild_id=guild_id, exclude_metadata=exclude_metadata
            )
            documents.append(doc)
        except Exception as exp:
            logging.error(
                f"Error while preparing documenet for message with id: {msg['messageId']}, exp: {exp}"
            )

    return documents


def prepare_document(
    message: dict[str, Any],
    guild_id: str,
    exclude_metadata: bool,
) -> Document | None:
    """
    prepare the llama_index.Document based on single discord message

    Parameters
    ------------
    message : dict[str, Any]
        the message of user in discord
    guild_id : str
        the guild id to access data
    exclude_metadata : bool
        whether to have all metadata (False) or have nothing (True)


    Returns
    --------
    doc : llama_index.Document
        a single prepared llama_index document
    """
    mention_ids = message["user_mentions"]
    role_ids = message["role_mentions"]
    author_id = message["author"]
    replier_id = message["replied_user"]
    reactions = message["reactions"]
    raw_content = message["content"]

    message_id = message["messageId"]
    channel_id = message["channelId"]
    thread_id = message["threadId"]

    mention_pattern = re.compile(r"<@&?\d+>")

    # Substitute the patterns with an empty string
    cleaned_message = mention_pattern.sub("", message["content"])
    if cleaned_message.strip() == "":
        raise ValueError("Message was just mentioning people or roles!")

    reaction_ids = prepare_raction_ids(reactions)

    mention_names: list[str]
    reactor_names: list[str]
    author_name: list[str]
    replier_name = None

    reaction_ids = remove_empty_str(reaction_ids)
    mention_ids = remove_empty_str(mention_ids)

    if replier_id is None:
        (
            (
                mention_names,
                reactor_names,
                author_name,
            ),
            (
                mention_global_names,
                reactor_global_names,
                author_global_name,
            ),
            (
                mention_nicknames,
                reactor_nicknames,
                author_nickname,
            ),
        ) = merge_user_ids_and_fetch_names(
            guild_id, mention_ids, reaction_ids, [author_id]
        )
    else:
        (
            (
                mention_names,
                reactor_names,
                author_name,
                replier_name,
            ),
            (
                mention_global_names,
                reactor_global_names,
                author_global_name,
                replier_global_name,
            ),
            (
                mention_nicknames,
                reactor_nicknames,
                author_nickname,
                replier_nickname,
            ),
        ) = merge_user_ids_and_fetch_names(
            guild_id, mention_ids, reaction_ids, [author_id], [replier_id]
        )

    role_names = convert_role_id(guild_id, role_ids)

    content = prepare_raw_message_ids(
        raw_content,
        roles=dict(zip(role_ids, role_names)),
        users=dict(zip(mention_ids, mention_names)),
    )
    # content_url_updated, url_reference = prepare_raw_message_urls(content)

    # always has length 1
    assert len(author_name) == 1, "Either None or multiple authors!"

    if thread_id is None:
        url = f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"
    else:
        url = f"https://discord.com/channels/{guild_id}/{thread_id}/{message_id}"

    name_fetcher = FetchDiscordChannelThreadNames(guild_id)
    msg_meta_data = {
        "channel": name_fetcher.fetch_discord_channel_name(channel_id),
        "date": message["createdDate"].strftime(DATE_FORMAT),
        "author_username": author_name[0],
        "url": url,
        # always including the thread_name, if `None`, then it was a channel message
        "thread": name_fetcher.fetch_discord_thread_name(thread_id) if thread_id else None,
    }
    if author_global_name[0] is not None:
        msg_meta_data["author_global_name"] = author_global_name[0]
    if author_nickname[0] is not None:
        msg_meta_data["author_nickname"] = author_nickname[0]

    if mention_names != []:
        msg_meta_data["mention_usernames"] = mention_names
        # removing `None` and updating the data
        mentions_gname = remove_none_from_list(mention_global_names)
        mentions_nickname = remove_none_from_list(mention_nicknames)
        if mentions_gname != []:
            msg_meta_data["mention_global_names"] = mentions_gname
        if mentions_nickname != []:
            msg_meta_data["mention_nicknames"] = mentions_nickname

    if reactor_names != []:
        msg_meta_data["reactors_username"] = reactor_names

        reactors_gname = remove_none_from_list(reactor_global_names)
        reactors_nickname = remove_none_from_list(reactor_nicknames)
        if reactors_gname != []:
            msg_meta_data["reactors_global_name"] = reactors_gname
        if reactors_nickname != []:
            msg_meta_data["reactors_nicknames"] = reactors_nickname
    # if url_reference != {}:
    #     msg_meta_data["url_reference"] = url_reference

    if replier_name is not None:
        msg_meta_data["replier_username"] = replier_name[0]
        if replier_global_name[0] is not None:
            msg_meta_data["replier_global_name"] = replier_global_name[0]
        if replier_nickname[0] is not None:
            msg_meta_data["replier_nickname"] = replier_nickname[0]
    if role_names != []:
        msg_meta_data["role_mentions"] = role_names

    # if content_url_updated == "":
    #     raise ValueError("Message with Empty content!")

    if not BasePreprocessor().extract_main_content(text=content):
        raise ValueError("Message didn't hold any valuable information!")

    # removing null characters
    content = re.sub(r"[\x00-\x1F\x7F]", "", content)

    doc: Document
    if not exclude_metadata:
        doc = Document(text=content, metadata=msg_meta_data)
        doc.excluded_embed_metadata_keys = [
            "channel",
            "date",
            "author_username",
            "author_global_name",
            "author_nickname",
            "mention_usernames",
            "mention_global_names",
            "mention_nicknames",
            "reactors_username",
            "reactors_global_name",
            "reactors_nicknames",
            "thread",
            # "url_reference",
            "replier_username",
            "replier_global_name",
            "replier_nickname",
            "role_mentions",
            "url",
        ]
        doc.excluded_llm_metadata_keys = [
            "author_nickname",
            "author_global_name",
            "mention_usernames",
            "mention_global_names",
            "mention_nicknames",
            "reactors_username",
            "reactors_global_name",
            "reactors_nicknames",
            "thread",
            # "url_reference",
            "replier_username",
            "replier_global_name",
            "replier_nickname",
            "role_mentions",
            "url",
        ]
    else:
        doc = Document(text=content)

    return doc
