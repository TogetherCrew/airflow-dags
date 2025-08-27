import logging
from datetime import datetime, timedelta
from typing import Any, Optional
from data_cleaning_utils.cleaning import (
    split_collection_name,
    group_and_merge_by_doc_id,
    build_documents,
    scroll_all_points,
)

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
        batch_size: int = int(conf.get("batch_size", params.get("batch_size", 512)))
        doc_batch_size: int = int(
            conf.get("doc_batch_size", params.get("doc_batch_size", 100))
        )
        reset_progress: bool = bool(conf.get("reset", params.get("reset", False)))

        community_id, platform_collection = split_collection_name(collection)
        logging.basicConfig(level=logging.INFO)
        logging.info(
            "Starting cleaning for collection=%s using model=%s", collection, model
        )

        client = QdrantSingleton.get_instance().get_client()

        # 1) Scroll points lazily and group/merge by doc_id without materializing all points
        merged_by_doc: dict[str, dict[str, Any]] = group_and_merge_by_doc_id(
            scroll_all_points(client, collection=collection, batch_size=batch_size)
        )
        doc_ids: list[str] = sorted(merged_by_doc.keys())
        total_docs = len(doc_ids)
        logging.info("Merged into %s documents by doc_id", total_docs)

        if not merged_by_doc:
            logging.info("No documents to process; exiting")
            return

        # 3) Determine resume index
        provided_resume_index = conf.get("resume_index")
        if reset_progress:
            resume_index = 0
        else:
            resume_index = (
                int(provided_resume_index)
                if provided_resume_index is not None
                else int(ti.xcom_pull(key="resume_index") or 0) if ti is not None
                else 0
            )

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

        ingestion_pipeline = CustomIngestionPipeline(
            community_id=community_id,
            collection_name=platform_collection,
            testing=False,
        )

        # 4) Loop through batches; after each successful batch, persist resume_index
        while resume_index < total_docs:
            batch_doc_ids = doc_ids[resume_index : resume_index + doc_batch_size]
            merged_subset: dict[str, dict[str, Any]] = {
                doc_id: merged_by_doc[doc_id] for doc_id in batch_doc_ids
            }

            documents: list[Document] = build_documents(
                merged=merged_subset, model=model
            )
            logging.info(
                "Built %s cleaned documents for current batch (%s..%s)",
                len(documents),
                resume_index,
                resume_index + len(batch_doc_ids) - 1,
            )

            ingestion_pipeline.run_pipeline(docs=documents)

            # advance the resume index only after successful ingestion
            resume_index += len(batch_doc_ids)
            if ti is not None:
                ti.xcom_push(key="resume_index", value=resume_index)
            logging.info(
                "Progress saved: processed %s/%s documents; resume_index=%s",
                resume_index,
                total_docs,
                resume_index,
            )

            # free up memory by removing the batch from the merged_by_doc dict
            merged_by_doc = {
                doc_id: merged_by_doc.pop(doc_id) for doc_id in batch_doc_ids
            }

        logging.info("All %s documents cleaned and re-ingested for %s", total_docs, collection)

    clean_and_reingest()


