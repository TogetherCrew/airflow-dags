import logging
import re
from typing import Any
import os
from concurrent.futures import ThreadPoolExecutor

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
from llama_index.core import Document
from tc_hivemind_backend.db.utils.preprocess_text import BasePreprocessor
from tc_hivemind_backend.db.mongo import MongoSingleton
from hivemind_etl_helpers.src.db.discord.utils.id_transform import convert_user_id


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
    logging.info("Building lookup maps to avoid per-message DB calls")
    (
        username_by_id,
        global_name_by_id,
        nickname_by_id,
        role_name_by_id,
        channel_name_by_id,
        thread_name_by_id,
    ) = _build_discord_lookup_maps(guild_id=guild_id, messages=messages)
    preprocessor = BasePreprocessor()

    documents: list[Document] = []
    mention_pattern = re.compile(r"<@&?\d+>")
    
    logging.info("Started preparing documents!")

    max_workers = min(32, max(4, (os.cpu_count() or 4) * 4))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        msg_ids: list[str] = []
        for msg in messages:
            futures.append(
                executor.submit(
                    _prepare_document_with_maps,
                    message=msg,
                    guild_id=guild_id,
                    exclude_metadata=exclude_metadata,
                    username_by_id=username_by_id,
                    global_name_by_id=global_name_by_id,
                    nickname_by_id=nickname_by_id,
                    role_name_by_id=role_name_by_id,
                    channel_name_by_id=channel_name_by_id,
                    thread_name_by_id=thread_name_by_id,
                    preprocessor=preprocessor,
                    mention_pattern=mention_pattern,
                )
            )
            msg_ids.append(msg.get("messageId"))

        for idx, fut in enumerate(futures):
            try:
                documents.append(fut.result())
            except Exception as exp:
                logging.error(
                    f"Error while preparing documenet for message with id: {msg_ids[idx]}, exp: {exp}"
                )

    logging.info("Documents prepared!")
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
        "date": message["createdDate"].timestamp(),
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
        doc = Document(
            text=content,
            metadata=msg_meta_data,
            doc_id=message_id,
        )
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
        doc = Document(
            text=content,
            doc_id=message_id,
        )

    return doc


def _build_discord_lookup_maps(
    guild_id: str, messages: list[dict]
) -> tuple[
    dict[str, str],
    dict[str, str],
    dict[str, str],
    dict[str, str],
    dict[str, str],
    dict[str, str],
]:
    """
    Build dictionaries for users, roles, channels, and threads to minimize DB calls.
    Returns:
      - username_by_id
      - global_name_by_id
      - nickname_by_id
      - role_name_by_id
      - channel_name_by_id
      - thread_name_by_id
    """
    user_ids: set[str] = set()
    role_ids: set[str] = set()
    channel_ids: set[str] = set()
    thread_ids: set[str] = set()

    for msg in messages:
        try:
            # Users
            mention_ids = remove_empty_str(msg.get("user_mentions", []))
            reaction_ids = remove_empty_str(
                prepare_raction_ids(msg.get("reactions", []))
            )
            author_id = msg.get("author")
            replier_id = msg.get("replied_user")

            user_ids.update(mention_ids)
            user_ids.update(reaction_ids)
            if author_id:
                user_ids.add(author_id)
            if replier_id:
                user_ids.add(replier_id)

            # Roles
            role_ids.update(remove_empty_str(msg.get("role_mentions", [])))

            # Channels/Threads
            channel_id = msg.get("channelId")
            thread_id = msg.get("threadId")
            if channel_id:
                channel_ids.add(channel_id)
            if thread_id:
                thread_ids.add(thread_id)
        except Exception:
            # Skip malformed message in map building; it will be handled later
            continue

    # Build user maps
    username_by_id: dict[str, str] = {}
    global_name_by_id: dict[str, str] = {}
    nickname_by_id: dict[str, str] = {}
    if user_ids:
        ordered_user_ids = list(user_ids)
        usernames, global_names, nicknames = convert_user_id(
            guild_id, ordered_user_ids
        )
        for i, uid in enumerate(ordered_user_ids):
            if i < len(usernames):
                username_by_id[uid] = usernames[i]
            if i < len(global_names):
                global_name_by_id[uid] = global_names[i]
            if i < len(nicknames):
                nickname_by_id[uid] = nicknames[i]

    # Build role map
    role_name_by_id: dict[str, str] = {}
    if role_ids:
        ordered_role_ids = list(role_ids)
        role_names = convert_role_id(guild_id, ordered_role_ids)
        for i, rid in enumerate(ordered_role_ids):
            if i < len(role_names):
                role_name_by_id[rid] = role_names[i]

    # Build channel/thread maps
    channel_name_by_id: dict[str, str] = {}
    thread_name_by_id: dict[str, str] = {}
    client = MongoSingleton.get_instance().get_client()
    if channel_ids:
        cursor = client[guild_id]["channels"].find(
            {"channelId": {"$in": list(channel_ids)}}, {"channelId": 1, "name": 1, "_id": 0}
        )
        for doc in cursor:
            cid = doc.get("channelId")
            name = doc.get("name")
            if cid and name:
                channel_name_by_id[cid] = name
    if thread_ids:
        cursor = client[guild_id]["threads"].find(
            {"id": {"$in": list(thread_ids)}}, {"id": 1, "name": 1, "_id": 0}
        )
        for doc in cursor:
            tid = doc.get("id")
            name = doc.get("name")
            if tid and name:
                thread_name_by_id[tid] = name

    return (
        username_by_id,
        global_name_by_id,
        nickname_by_id,
        role_name_by_id,
        channel_name_by_id,
        thread_name_by_id,
    )


def _prepare_document_with_maps(
    message: dict[str, Any],
    guild_id: str,
    exclude_metadata: bool,
    username_by_id: dict[str, str],
    global_name_by_id: dict[str, str],
    nickname_by_id: dict[str, str],
    role_name_by_id: dict[str, str],
    channel_name_by_id: dict[str, str],
    thread_name_by_id: dict[str, str],
    preprocessor: BasePreprocessor,
    mention_pattern: re.Pattern[str],
) -> Document:
    mention_ids = message["user_mentions"]
    role_ids = message["role_mentions"]
    author_id = message["author"]
    replier_id = message["replied_user"]
    reactions = message["reactions"]
    raw_content = message["content"]

    message_id = message["messageId"]
    channel_id = message["channelId"]
    thread_id = message["threadId"]

    # Substitute the patterns with an empty string
    cleaned_message = mention_pattern.sub("", raw_content)
    if cleaned_message.strip() == "":
        raise ValueError("Message was just mentioning people or roles!")

    reaction_ids = remove_empty_str(prepare_raction_ids(reactions))
    mention_ids = remove_empty_str(mention_ids)

    # Resolve users from maps
    mention_names = [username_by_id.get(u) for u in mention_ids if username_by_id.get(u) is not None]
    mention_global_names = [global_name_by_id.get(u) for u in mention_ids if global_name_by_id.get(u) is not None]
    mention_nicknames = [nickname_by_id.get(u) for u in mention_ids if nickname_by_id.get(u) is not None]

    reactor_names = [username_by_id.get(u) for u in reaction_ids if username_by_id.get(u) is not None]
    reactor_global_names = [global_name_by_id.get(u) for u in reaction_ids if global_name_by_id.get(u) is not None]
    reactor_nicknames = [nickname_by_id.get(u) for u in reaction_ids if nickname_by_id.get(u) is not None]

    author_name = [username_by_id.get(author_id)] if username_by_id.get(author_id) is not None else []
    author_global_name = [global_name_by_id.get(author_id)] if global_name_by_id.get(author_id) is not None else [None]
    author_nickname = [nickname_by_id.get(author_id)] if nickname_by_id.get(author_id) is not None else [None]

    replier_name = None
    replier_global_name = [None]
    replier_nickname = [None]
    if replier_id is not None:
        replier_name = [username_by_id.get(replier_id)] if username_by_id.get(replier_id) is not None else None
        replier_global_name = [global_name_by_id.get(replier_id)]
        replier_nickname = [nickname_by_id.get(replier_id)]

    role_names = [role_name_by_id.get(r) for r in role_ids if role_name_by_id.get(r) is not None]

    content = prepare_raw_message_ids(
        raw_content,
        roles=dict(zip(role_ids, role_names)),
        users=dict(zip(mention_ids, mention_names)),
    )

    # always has length 1
    assert len(author_name) == 1, "Either None or multiple authors!"

    if thread_id is None:
        url = f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"
    else:
        url = f"https://discord.com/channels/{guild_id}/{thread_id}/{message_id}"

    msg_meta_data = {
        "channel": channel_name_by_id.get(channel_id),
        "date": message["createdDate"].timestamp(),
        "author_username": author_name[0],
        "url": url,
        # always including the thread_name, if `None`, then it was a channel message
        "thread": thread_name_by_id.get(thread_id) if thread_id else None,
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

    if replier_name is not None:
        if replier_name and replier_name[0] is not None:
            msg_meta_data["replier_username"] = replier_name[0]
        if replier_global_name[0] is not None:
            msg_meta_data["replier_global_name"] = replier_global_name[0]
        if replier_nickname[0] is not None:
            msg_meta_data["replier_nickname"] = replier_nickname[0]
    if role_names != []:
        msg_meta_data["role_mentions"] = role_names

    if not preprocessor.extract_main_content(text=content):
        raise ValueError("Message didn't hold any valuable information!")

    # removing null characters
    content = re.sub(r"[\x00-\x1F\x7F]", "", content)

    if not exclude_metadata:
        doc = Document(
            text=content,
            metadata=msg_meta_data,
            doc_id=message_id,
        )
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
        return doc
    else:
        return Document(
            text=content,
            doc_id=message_id,
        )
