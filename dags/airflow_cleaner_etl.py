import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from airflow import DAG
from airflow.decorators import task
from airflow.configuration import conf as airflow_conf


with DAG(
    dag_id="airflow_logs_cleanup",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    params={
        "max_age_days": 30,  # delete files older than N days
        "dry_run": False,  # when True, only report, do not delete
        "remove_empty_dirs": True,  # remove empty directories after deletion
        "base_log_folder": None,  # override Airflow BASE_LOG_FOLDER if needed
    },
) as dag:

    @task(retries=0)
    def clean_airflow_logs(**kwargs) -> None:
        """
        Delete Airflow log files older than the configured max_age_days.
        Respects Airflow's logging BASE_LOG_FOLDER by default. Supports dry-run.
        """
        logging.basicConfig(level=logging.INFO)

        run_conf = kwargs.get("dag_run").conf if "dag_run" in kwargs else {}
        params = kwargs.get("params", {})

        # Resolve configuration
        configured_base: Optional[str] = run_conf.get("base_log_folder") or params.get("base_log_folder")
        # In Airflow 2.x, the option name is 'base_log_folder' (lowercase)
        base_log_folder: str = configured_base or airflow_conf.get("logging", "base_log_folder")
        max_age_days: int = int(run_conf.get("max_age_days", params.get("max_age_days", 30)))
        dry_run: bool = bool(run_conf.get("dry_run", params.get("dry_run", False)))
        remove_empty_dirs: bool = bool(
            run_conf.get("remove_empty_dirs", params.get("remove_empty_dirs", True))
        )

        log_root = Path(base_log_folder).expanduser().resolve()
        if not log_root.exists() or not log_root.is_dir():
            raise FileNotFoundError(f"Log folder not found or not a directory: {log_root}")

        cutoff_seconds = max_age_days * 24 * 60 * 60
        now = time.time()
        cutoff_ts = now - cutoff_seconds

        logging.info(
            "Scanning for files older than %s days in %s (dry_run=%s)",
            max_age_days,
            str(log_root),
            dry_run,
        )

        total_files_scanned = 0
        candidate_files = 0
        deleted_files = 0
        bytes_freed = 0

        # Walk the directory tree without following symlinks
        for root, dirs, files in os.walk(log_root, topdown=True, followlinks=False):
            # Optional: prune hidden directories quickly (keeps traversal small)
            dirs[:] = [d for d in dirs if not d.startswith(".")]

            for file_name in files:
                try:
                    total_files_scanned += 1
                    file_path = Path(root) / file_name
                    # Skip symlinks and non-regular files
                    try:
                        if file_path.is_symlink() or not file_path.is_file():
                            continue
                    except OSError:
                        # In case of broken links or permission issues on lstat
                        continue

                    try:
                        stat_result = file_path.stat()
                    except FileNotFoundError:
                        # File may have been rotated/deleted concurrently
                        continue

                    if stat_result.st_mtime <= cutoff_ts:
                        candidate_files += 1
                        if not dry_run:
                            try:
                                bytes_freed += stat_result.st_size
                                file_path.unlink(missing_ok=True)
                                deleted_files += 1
                            except PermissionError:
                                logging.warning("Permission denied deleting %s", file_path)
                            except OSError as err:
                                logging.warning("Failed deleting %s: %s", file_path, err)
                except Exception as err:
                    logging.warning("Unexpected error processing %s/%s: %s", root, file_name, err)

        logging.info(
            "Scan complete. files_scanned=%s candidates=%s deleted=%s bytes_freed=%s",
            total_files_scanned,
            candidate_files,
            deleted_files,
            bytes_freed,
        )

        if remove_empty_dirs and not dry_run:
            # Remove empty directories bottom-up
            removed_dirs = 0
            for root, dirs, _ in os.walk(log_root, topdown=False):
                for d in dirs:
                    dir_path = Path(root) / d
                    try:
                        # Only remove if empty
                        if not any(dir_path.iterdir()):
                            dir_path.rmdir()
                            removed_dirs += 1
                    except OSError:
                        # Skip non-empty or permission issues
                        pass
            logging.info("Removed %s empty directories", removed_dirs)

    clean_airflow_logs()


