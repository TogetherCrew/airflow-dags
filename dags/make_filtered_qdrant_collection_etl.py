import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from qdrant_client import models

from tc_hivemind_backend.db.qdrant import QdrantSingleton


with DAG(
    dag_id="make_filtered_qdrant_collection",
    start_date=datetime(2025, 8, 26),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    params={
        "dst": None,  # defaults to f"{src}_filtered_backup" when not provided
        "target_ts": 1735689600,  # 2025-01-01 00:00:00
        "batch": 50,
        "reset": False,  # when True, (re)create destination before copying
    },
) as dag:

    @task(retries=3, retry_delay=timedelta(minutes=2))
    def copy_filtered_collection(**kwargs):
        """
        Copy points from a source Qdrant collection to a destination collection,
        filtered by payload field "date" >= target_ts.

        Parameters (via dag_run.conf or DAG params):
        - src: source collection name
        - dst: destination collection name (defaults to f"{src}_filtered_backup")
        - target_ts: numeric timestamp threshold (inclusive)
        - batch: scroll/upsert batch size
        """

        # Resolve run-time configuration with DAG params as fallback
        conf = kwargs.get("dag_run").conf if "dag_run" in kwargs else {}
        params = kwargs.get("params", {})

        src = conf.get("src")
        dst = conf.get("dst") or params.get("dst")
        target_ts = conf.get("target_ts", params.get("target_ts", 1735689600))
        batch = conf.get("batch", params.get("batch", 50))
        reset = bool(conf.get("reset", params.get("reset", False)))
        provided_resume_offset = conf.get("resume_offset")  # optional manual resume cursor

        if not src:
            raise ValueError("'src' collection name must be provided via params!")
        if not dst:
            dst = f"{src}_filtered_backup"

        target_ts = float(target_ts)
        batch = int(batch)

        logging.basicConfig(level=logging.INFO)
        client = QdrantSingleton.get_instance().get_client()
        ti = kwargs.get("ti")

        # --- 0) Define filter on the numeric payload key "date" ---
        eps = 1e-6
        date_filter = models.FieldCondition(
            key="date",
            range=models.Range(gte=target_ts - eps),
        )
        flt = models.Filter(must=[date_filter])

        # --- 1) Ensure destination collection exists with the SAME vector schema ---
        src_info = client.get_collection(src)
        vectors_cfg = src_info.config.params.vectors
        shards = src_info.config.params.shard_number
        replication = getattr(src_info.config.params, "replication_factor", None)

        existing_collections = [c.name for c in client.get_collections().collections]
        if reset or dst not in existing_collections:
            client.recreate_collection(
                collection_name=dst,
                vectors_config=vectors_cfg,
                shard_number=shards,
                replication_factor=replication,
            )
            logging.info("Destination collection %s (re)created", dst)
        else:
            logging.info("Destination collection %s already exists; skipping recreate", dst)

        # Optional: speeds up filtered scrolling on numeric payloads
        try:
            client.create_payload_index(
                collection_name=src,
                field_name="date",
                field_schema=models.PayloadSchemaType.FLOAT,
            )
        except Exception:  # noqa: BLE001 - best-effort index creation
            pass

        # --- 2) Scroll + copy in batches (vectors + payloads), with resumable offset ---
        # Determine resume offset: explicit conf overrides, otherwise XCom from previous try
        resume_offset = provided_resume_offset
        if resume_offset is None and ti is not None:
            resume_offset = ti.xcom_pull(key="resume_offset")
        logging.info("Starting scroll with resume_offset=%s", resume_offset)

        while True:
            # Fetch a batch using the current resume_offset
            points, next_offset = client.scroll(
                collection_name=src,
                scroll_filter=flt,
                with_vectors=True,
                with_payload=True,
                limit=batch,
                offset=resume_offset,
            )
            logging.info("Fetched %s points; next_offset=%s", len(points), next_offset)
            if not points:
                break

            # Upsert the fetched batch; only advance resume_offset after success
            client.upsert(
                collection_name=dst,
                points=models.Batch(
                    ids=[p.id for p in points],
                    vectors=[p.vector for p in points],
                    payloads=[p.payload for p in points],
                ),
                wait=True,
            )

            # Persist the safe resume position so retries/next runs can continue
            resume_offset = next_offset
            if ti is not None:
                ti.xcom_push(key="resume_offset", value=resume_offset)
            logging.info("Progress saved; resume_offset updated to %s", resume_offset)

        # (Optional) kick optimizers so the index is ready before snapshotting
        client.update_collection(
            collection_name=dst,
            optimizer_config=models.OptimizersConfigDiff(indexing_threshold=0),
        )

        # --- 3) Create a snapshot of ONLY the filtered collection ---
        snap = client.create_snapshot(collection_name=dst, wait=True)
        logging.info("Snapshot created: %s", snap)

    copy_filtered_collection()
