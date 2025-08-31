import logging
from datetime import datetime, timezone
from typing import Optional

from bson import ObjectId

try:
    # Prefer the runtime-installed backend module
    from tc_hivemind_backend.db.mongo import MongoSingleton
except Exception as import_error:  # pragma: no cover - defensive import
    raise import_error



class ResumeState:
    """Helper to persist and retrieve resume progress for a platform.

    Stores progress under Core.platforms.metadata.resume_index.
    """

    def __init__(self, platform_id: str) -> None:
        self.platform_id = platform_id
        self.collection = MongoSingleton.get_instance().get_client()["Core"]["platforms"]

    def get(self) -> int:
        """Return the stored resume_index or 0 if missing/error."""
        try:
            doc = self.collection.find_one(
                {"_id": ObjectId(self.platform_id)},
                {"_id": 0, "metadata.resume_index": 1},
            )
            if not doc:
                return 0
            meta = doc.get("metadata") or {}
            value = meta.get("resume_index")
            if value is None:
                return 0
            try:
                return int(value)
            except Exception:
                logging.warning(
                    "Invalid resume_index type in metadata for %s: %r",
                    self.platform_id,
                    value,
                )
                return 0
        except Exception as exp:  # pragma: no cover - best-effort read
            logging.warning(
                "Failed to read resume_index from MongoDB for %s: %s",
                self.platform_id,
                exp,
            )
            return 0

    def set(self, index: int) -> Optional[int]:
        """Persist resume_index and updatedAt. Returns written index or None on error."""
        try:
            self.collection.update_one(
                {"_id": ObjectId(self.platform_id)},
                {
                    "$set": {
                        "metadata.resume_index": int(index),
                        "updatedAt": datetime.now(tz=timezone.utc),
                    }
                },
            )
            return int(index)
        except Exception as exp:  # pragma: no cover - best-effort write
            logging.warning(
                "Failed to write resume_index=%s to MongoDB for %s: %s",
                index,
                self.platform_id,
                exp,
            )
            return None

    def reset(self) -> bool:
        """Remove stored resume_index from MongoDB. Returns True on success, False on error."""
        try:
            self.collection.update_one(
                {"_id": ObjectId(self.platform_id)},
                {
                    "$unset": {"metadata.resume_index": ""},
                    "$set": {"updatedAt": datetime.now(tz=timezone.utc)},
                },
            )
            return True
        except Exception as exp:  # pragma: no cover - best-effort write
            logging.warning(
                "Failed to reset resume_index in MongoDB for %s: %s",
                self.platform_id,
                exp,
            )
            return False
