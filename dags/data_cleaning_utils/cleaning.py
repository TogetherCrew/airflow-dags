import logging
from collections import defaultdict
from typing import Any, Iterable

import json
from llama_index.core import Document
from qdrant_client import QdrantClient


def split_collection_name(full_collection: str) -> tuple[str, str]:
    """
    Split the full Qdrant collection name into community_id and collection_name.

    Parameters
    ----------
    full_collection : str
        The Qdrant collection name in the format "{community_id}_{collection_name}".

    Returns
    -------
    tuple[str, str]
        community_id, collection_name
    """
    if "_" not in full_collection:
        logging.warning(
            "Collection name '%s' has no underscore; using community_id='%s' and empty collection_name.",
            full_collection,
            full_collection,
        )
        return full_collection, ""

    community_id, remainder = full_collection.split("_", 1)
    if not community_id:
        raise ValueError("Invalid collection name: community_id part is empty.")
    if not remainder:
        logging.warning(
            "Collection name '%s' has empty collection part; proceeding with empty collection_name.",
            full_collection,
        )
        return community_id, ""
    return community_id, remainder


def extract_text_and_order(payload: dict) -> tuple[str, int | None]:
    """
    Extract the textual content and an optional ordering key from a Qdrant point payload.

    Parameters
    ----------
    payload : dict
        The payload of a Qdrant point.

    Returns
    -------
    tuple[str, Optional[int]]
        The node text and the start_char_idx (if available) to maintain order.
    """
    text_value: str = ""
    order_key: int | None = None

    # Preferred: parse serialized llama-index node under "_node_content"
    node_blob = payload.get("_node_content")
    if isinstance(node_blob, str):
        try:
            node_json = json.loads(node_blob)
            text_value = node_json.get("text", "") or ""
            order_key = node_json.get("start_char_idx")
        except Exception:  # noqa: BLE001 - best-effort parsing
            text_value = ""

    # Fallback: sometimes text may be present at top-level payload
    if not text_value:
        text_value = payload.get("text", "") or ""

    return text_value, order_key


def group_and_merge_by_doc_id(points: Iterable) -> dict[str, dict[str, Any]]:
    """
    Group all nodes by their doc_id and merge their texts in order.

    Parameters
    ----------
    points : Iterable
        Iterable of Qdrant points with payloads containing 'doc_id' and '_node_content'.

    Returns
    -------
    dict[str, dict]
        Mapping of doc_id -> { "text": merged text, "metadata": original payload metadata }.
    """
    grouped: dict[str, list[tuple[int, str]]] = defaultdict(list)
    metadata_by_doc: dict[str, dict[str, Any]] = {}

    for p in points:
        payload = getattr(p, "payload", None) or {}
        doc_id = payload.get("doc_id") or payload.get("document_id") or payload.get("ref_doc_id")
        if not doc_id:
            # Skip nodes without a doc identifier
            continue
        # Ensure doc_id is coerced to string immediately after retrieval
        doc_id_str = str(doc_id)

        text, order_key = extract_text_and_order(payload)
        if not text:
            continue

        # Use a stable fallback order if start_char_idx is missing
        order_index = order_key if isinstance(order_key, int) else len(grouped[doc_id_str])
        grouped[doc_id_str].append((order_index, text))

        # Record metadata once per doc_id (all points share same metadata per user's note)
        if doc_id_str not in metadata_by_doc:
            # Keep all payload fields to preserve metadata and normalize doc_id
            meta_copy = dict(payload)
            meta_copy["doc_id"] = doc_id_str
            metadata_by_doc[doc_id_str] = meta_copy

    merged_with_meta: dict[str, dict[str, Any]] = {}
    for doc_id, chunks in grouped.items():
        chunks.sort(key=lambda t: t[0])
        merged_text = "\n\n".join(chunk for _, chunk in chunks)
        merged_with_meta[doc_id] = {
            "text": merged_text,
            "metadata": metadata_by_doc.get(doc_id, {}),
        }

    return merged_with_meta


def clean_text_with_llm(raw_text: str, model: str = "gpt-5-nano-2025-08-07") -> str:
    """
    Clean text using an LLM model. If the model call fails, return the raw text.

    Parameters
    ----------
    raw_text : str
        The input text to be cleaned.
    model : str
        The model name to use for cleaning (default: 'gpt-5-nano-2025-08-07').

    Returns
    -------
    str
        The cleaned text or the original on failure.
    """
    try:
        from openai import OpenAI  # Imported lazily to keep DAG import-time lean

        client = OpenAI()

        system_prompt = ( 
            "You are a data-cleaning assistant for preparing text for a retrieval-augmented generation (RAG) pipeline.\n"
            "Your job is to extract **informative, knowledge-rich content** and remove noise.\n\n"
            "* **Remove:** navigation menus, disclaimers, ads, cookie notices, boilerplate, irrelevant chatter, emojis, hashtags, greetings, spam, or repeated filler.\n"
            "* **Keep:** explanations, facts, product/service info, FAQs, instructions, discussions containing substantive knowledge, or any text with unique informational value.\n"
            "* Output should be **clean, structured, and concise**, ready for semantic search indexing.\n"
            "* Do not add new information and just return the cleaned text."
        )

        user_prompt = (
            "Here is some raw text. Please clean it according to the above instructions:"
            f"\n\n```\n{raw_text}\n```"
        )

        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        cleaned = response.choices[0].message.content or ""
        return cleaned.strip() or raw_text
    except Exception as exp:  # noqa: BLE001 - best-effort LLM call
        logging.warning("LLM cleaning failed; falling back to raw text: %s", exp)
        return raw_text


def build_documents(merged: dict[str, dict[str, Any]], model: str) -> list[Document]:
    """
    Build llama-index Document objects for cleaned texts.

    Parameters
    ----------
    merged : dict[str, dict]
        Mapping of doc_id -> { "text": merged raw text, "metadata": original payload metadata }.
    model : str
        The LLM model name to use for cleaning.

    Returns
    -------
    list[llama_index.core.Document]
        A list of Documents with cleaned text and original doc_id.
    """
    documents: list[Document] = []
    for doc_id, bundle in merged.items():
        raw_text: str = bundle.get("text", "")
        original_metadata: dict[str, Any] = dict(bundle.get("metadata", {}))

        cleaned = clean_text_with_llm(raw_text=raw_text, model=model)

        # Ensure doc_id is present and mark as cleaned without removing any existing fields
        if "doc_id" not in original_metadata:
            original_metadata["doc_id"] = str(doc_id)
        original_metadata["cleaned"] = True

        documents.append(
            Document(
                text=cleaned,
                id_=str(doc_id),
                metadata=original_metadata,
            )
        )
    return documents


def scroll_all_points(client: QdrantClient, collection: str, batch_size: int = 512) -> Iterable:
    """
    Generator that yields all points from a Qdrant collection.

    Parameters
    ----------
    client : qdrant_client.QdrantClient
        The Qdrant client instance.
    collection : str
        The collection name to scroll.
    batch_size : int
        The number of points to fetch per scroll call.

    Yields
    ------
    qdrant_client.http.models.Record
        Point records with payloads.
    """
    offset = None
    while True:
        points, next_offset = client.scroll(
            collection_name=collection,
            with_vectors=False,
            with_payload=True,
            limit=batch_size,
            offset=offset,
        )
        if not points:
            break
        yield points

        # Break when there is no next page
        if next_offset is None:
            break
        # Safety guard: if server returns the same offset, stop to avoid infinite loop
        if next_offset == offset:
            logging.warning("Qdrant scroll returned the same offset; breaking to avoid infinite loop.")
            break
        offset = next_offset
