import logging
from datetime import datetime, timedelta
from typing import Any, Optional
from concurrent.futures import ThreadPoolExecutor, Future
from data_cleaning_utils.cleaning import (
    split_collection_name,
    group_and_merge_by_doc_id,
    build_documents,
    scroll_all_points,
)
from data_cleaning_utils.resume_state import ResumeState

from airflow import DAG
from airflow.decorators import task
from llama_index.core import Document
from dotenv import load_dotenv


try:
    # Prefer the runtime-installed backend module
    from tc_hivemind_backend.db.qdrant import QdrantSingleton
    from tc_hivemind_backend.ingest_qdrant import CustomIngestionPipeline
except Exception as import_error:  # pragma: no cover - defensive import
    raise import_error



with DAG(
    dag_id="data_cleaning_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    params={
        "collection": None,  # e.g., "<community_id>_<platform_name>"
        "model": "gpt-5-nano-2025-08-07",
        "batch_size": 128,
        "doc_batch_size": 100,  # number of doc_ids to clean and ingest per batch
        "max_workers": 8,  # parallel LLM workers
        "min_chars_for_llm": 0,  # skip LLM for shorter texts
        "sort_doc_ids": True,  # set False to skip sorting for speed
        "platform_id": None,  # optional: persist resume_index in Core.platforms
        "reset": False,  # reset resume progress
    },
) as dag:

    @task(retries=3, retry_delay=timedelta(minutes=2))
    def clean_and_reingest(**kwargs) -> None:
        """
        End-to-end task: fetch, merge by doc_id, clean using LLM, and re-ingest.

        Expects params or dag_run.conf to provide:
        - collection: full qdrant collection name "{community_id}_{collection_name}"
        - model: LLM model name (defaults to 'gpt-5-nano-2025-08-07')
        - batch_size: scroll batch size (optional)
        - doc_batch_size: number of doc_ids per cleaning/ingest batch (optional)
        - reset: when true, resets resume index
        """
        load_dotenv()
        conf = kwargs.get("dag_run").conf if "dag_run" in kwargs else {}
        params = kwargs.get("params", {})
        ti = kwargs.get("ti")

        collection: Optional[str] = conf.get("collection") or params.get("collection")
        if not collection:
            raise ValueError("'collection' must be provided via params or conf")

        model: str = conf.get("model", params.get("model", "gpt-5-nano-2025-08-07"))
        batch_size: int = int(conf.get("batch_size", params.get("batch_size", 128)))
        doc_batch_size: int = int(
            conf.get("doc_batch_size", params.get("doc_batch_size", 100))
        )
        max_workers: int = int(conf.get("max_workers", params.get("max_workers", 8)))
        min_chars_for_llm: int = int(
            conf.get("min_chars_for_llm", params.get("min_chars_for_llm", 0))
        )
        sort_doc_ids: bool = bool(
            conf.get("sort_doc_ids", params.get("sort_doc_ids", True))
        )
        reset_progress: bool = bool(conf.get("reset", params.get("reset", False)))
        platform_id: Optional[str] = conf.get("platform_id", params.get("platform_id"))

        community_id, platform_collection = split_collection_name(collection)
        logging.basicConfig(level=logging.INFO)
        logging.info(
            "Starting cleaning for collection=%s using model=%s", collection, model
        )

        client = QdrantSingleton.get_instance().get_client()

        # 1) Scroll points lazily and group/merge by doc_id without materializing all points
        merged_by_doc: dict[str, dict[str, Any]] = group_and_merge_by_doc_id(
            scroll_all_points(client, collection=collection, batch_size=batch_size), reset=reset_progress
        )
        doc_ids: list[str]
        if sort_doc_ids:
            doc_ids = sorted(merged_by_doc.keys())
        else:
            doc_ids = list(merged_by_doc.keys())
        total_docs = len(doc_ids)
        logging.info("Merged into %s documents by doc_id", total_docs)

        if not merged_by_doc:
            logging.info("No documents to process; exiting")
            return

        # 3) Determine resume index (conf > Mongo > XCom), and honor reset state
        provided_resume_index = conf.get("resume_index")

        # Persist resume in Mongo under platforms.metadata.resume_index, keyed by platform_collection
        # platform_collection is used as platform_id (expected to be ObjectId string when available)
        resume_store = ResumeState(platform_id=platform_collection)

        if reset_progress:
            # reset both Mongo and XCom state
            resume_store.reset()
            if ti is not None:
                ti.xcom_push(key="resume_index", value=0)
            resume_index = 0
        else:
            if provided_resume_index is not None:
                resume_index = int(provided_resume_index)
            else:
                mongo_resume = resume_store.get()
                if mongo_resume:
                    resume_index = int(mongo_resume)
                else:
                    resume_index = int(ti.xcom_pull(key="resume_index") or 0) if ti is not None else 0

        # If a platform_id is provided, prefer MongoDB stored progress unless overridden
        if not reset_progress and platform_id and provided_resume_index is None:
            mongo_resume = resume_store.get()
            if mongo_resume and mongo_resume > resume_index:
                resume_index = mongo_resume

        if resume_index < 0 or resume_index > total_docs:
            logging.warning(
                "Resume index %s out of range for %s docs; resetting to 0",
                resume_index,
                total_docs,
            )
            resume_index = 0

        logging.info(
            "Processing documents in batches of %s starting at index %s",
            doc_batch_size,
            resume_index,
        )

        # 4) Loop through batches with pipelined ingestion; overlap next batch build while previous ingests
        ingest_pool = ThreadPoolExecutor(max_workers=1)
        prev_future: Optional[Future] = None
        prev_existing_ids: list[str] = []
        prev_batch_len: int = 0

        def _ingest_batch(docs: list[Document]) -> None:
            pipe = CustomIngestionPipeline(
                community_id=community_id,
                collection_name=platform_collection,
                testing=False,
                use_cache=False,
            )
            pipe.run_pipeline(docs=docs)

        try:
            while resume_index < total_docs:
                batch_doc_ids = doc_ids[resume_index : resume_index + doc_batch_size]
                # Guard against missing doc_ids due to prior mutations or data issues
                existing_ids = [doc_id for doc_id in batch_doc_ids if doc_id in merged_by_doc]
                if len(existing_ids) < len(batch_doc_ids):
                    missing = set(batch_doc_ids) - set(existing_ids)
                    logging.warning(
                        "Skipping %s missing doc_ids in current batch: %s",
                        len(missing),
                        list(missing)[:5],
                    )

                merged_subset: dict[str, dict[str, Any]] = {
                    doc_id: merged_by_doc[doc_id] for doc_id in existing_ids
                }

                if not merged_subset:
                    logging.info("No available documents in this batch; advancing resume_index")
                    resume_index += len(batch_doc_ids)
                    if ti is not None:
                        ti.xcom_push(key="resume_index", value=resume_index)
                    continue

                documents: list[Document] = build_documents(
                    merged=merged_subset,
                    model=model,
                    max_workers=max_workers,
                    min_chars_for_llm=min_chars_for_llm,
                )
                logging.info(
                    "Built %s cleaned documents for current batch (%s..%s)",
                    len(documents),
                    resume_index,
                    resume_index + len(batch_doc_ids) - 1,
                )

                # If a previous ingestion is in flight, wait for completion, then persist progress
                if prev_future is not None:
                    prev_future.result()
                    resume_index += prev_batch_len
                    if ti is not None:
                        ti.xcom_push(key="resume_index", value=resume_index)
                    resume_store.set(resume_index)
                    if platform_id:
                        resume_store.set(resume_index)
                    logging.info(
                        "Progress saved: processed %s/%s documents; resume_index=%s",
                        resume_index,
                        total_docs,
                        resume_index,
                    )
                    # Free memory of the completed batch
                    for did in prev_existing_ids:
                        merged_by_doc.pop(did, None)

                # Submit current batch ingestion asynchronously
                prev_future = ingest_pool.submit(_ingest_batch, documents)
                prev_existing_ids = existing_ids
                prev_batch_len = len(batch_doc_ids)

                # Loop continues to build the next batch while current ingests

        finally:
            # Ensure last batch (if any) is committed and persisted
            if prev_future is not None:
                prev_future.result()
                resume_index += prev_batch_len
                if ti is not None:
                    ti.xcom_push(key="resume_index", value=resume_index)
                resume_store.set(resume_index)
                if platform_id:
                    resume_store.set(resume_index)
                logging.info(
                    "Final progress saved: processed %s/%s documents; resume_index=%s",
                    resume_index,
                    total_docs,
                    resume_index,
                )
                for did in prev_existing_ids:
                    merged_by_doc.pop(did, None)
            ingest_pool.shutdown(wait=True)

        logging.info("All %s documents cleaned and re-ingested for %s", total_docs, collection)

    clean_and_reingest()


