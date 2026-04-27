"""Database interface for persisted scan data."""

import logging
import time

from pymongo import ASCENDING, DESCENDING
from tornado import web
from wipac_dev_tools.mongo_jsonschema_tools import (
    MongoDoc,
    MongoJSONSchemaValidatedCollection,
)

from ..config import ENV, SCAN_MIN_PRIORITY_TO_START_ASAP

LOGGER = logging.getLogger(__name__)


# -----------------------------------------------------------------------------


class ManifestHelper:
    """Houses advanced methods for interacting with the Manifest collection."""

    @staticmethod
    def _validate_event_metadata(
        in_db: MongoDoc,
        upserting: MongoDoc,
        scan_id: str,
        event_metadata: MongoDoc,
    ) -> None:
        if not event_metadata:
            raise ValueError("event_metadata cannot be falsy")
        elif not in_db["event_metadata"]:
            upserting["event_metadata"] = event_metadata
        elif in_db["event_metadata"] != event_metadata:
            msg = "Cannot change an existing event_metadata"
            raise web.HTTPError(
                400,
                log_message=msg + f" for {scan_id=}",
                reason=msg,
            )

    @staticmethod
    def _validate_scan_metadata(
        in_db: MongoDoc,
        upserting: MongoDoc,
        scan_id: str,
        scan_metadata: MongoDoc,
    ) -> None:
        if not scan_metadata:
            raise ValueError("scan_metadata cannot be falsy")
        elif not in_db["scan_metadata"]:
            upserting["scan_metadata"] = scan_metadata
        elif in_db["scan_metadata"] != scan_metadata:
            msg = "Cannot change an existing scan_metadata"
            raise web.HTTPError(
                400,
                log_message=msg + f" for {scan_id=}",
                reason=msg,
            )

    @staticmethod
    async def patch(
        collection: MongoJSONSchemaValidatedCollection,
        scan_id: str,
        progress: MongoDoc | None = None,
        event_metadata: MongoDoc | None = None,
        scan_metadata: MongoDoc | None = None,
    ) -> MongoDoc:
        """Update `progress` at doc matching `scan_id`."""
        LOGGER.debug(f"patching manifest for {scan_id=}")

        if not (progress or event_metadata or scan_metadata):
            LOGGER.debug(f"nothing to patch for manifest ({scan_id=})")
            return await collection.find_one({"scan_id": scan_id})

        upserting: MongoDoc = {}
        if progress:
            # fill in some defaults
            progress["processing_stats"] = {
                "rate": {},
                "end": "",
                "finished": False,
                "predictions": {},  # open to requestor
                **progress["processing_stats"],  # fyi: this overrides any defaults
            }
            progress["start"] = progress.get("start", None)
            progress["end"] = progress.get("end", None)
            upserting["progress"] = progress

        # Validate, then store
        # NOTE: in theory there's a race condition (get+upsert)
        in_db = await collection.find_one({"scan_id": scan_id})
        if event_metadata:
            ManifestHelper._validate_event_metadata(
                in_db, upserting, scan_id, event_metadata
            )
        if scan_metadata:
            ManifestHelper._validate_scan_metadata(
                in_db, upserting, scan_id, scan_metadata
            )

        # Update db
        if not upserting:  # did we actually update anything?
            LOGGER.debug(f"nothing to patch for manifest ({scan_id=})")
            return in_db
        else:
            return await collection.find_one_and_update(
                {"scan_id": scan_id},
                {"$set": upserting},
            )


# -----------------------------------------------------------------------------


class ScanBacklogHelper:
    """Houses advanced methods for interacting with the ScanBacklog collection."""

    @staticmethod
    async def fetch_next_as_pending(
        collection: MongoJSONSchemaValidatedCollection,
        include_low_priority_scans: bool,
    ) -> MongoDoc:
        """Fetch the next ready entry and mark as pending.

        This for when the container is restarted (process is killed).
        """
        # LOGGER.debug("fetching & marking top backlog entry as a pending...")
        # ^^^ don't log too often

        mongo_filter = {
            # get entries that have never been pending (0.0) and/or
            # entries that have been pending for too long (parent
            # process may have died) -- younger pending entries may
            # still be in flight by other processes)
            "pending_timestamp": {
                "$lt": time.time() - ENV.SCAN_BACKLOG_PENDING_ENTRY_TTL_REVIVE
            }
        }
        if not include_low_priority_scans:
            # iow: only include high priority scans
            mongo_filter.update({"priority": {"$gte": SCAN_MIN_PRIORITY_TO_START_ASAP}})

        # atomically find & update; raises DocumentNotFoundException if no match
        entry = await collection.find_one_and_update(
            mongo_filter,
            {
                "$set": {"pending_timestamp": time.time()},
                "$inc": {"next_attempt": 1},
            },
            sort=[
                ("priority", DESCENDING),  # highest first
                ("timestamp", ASCENDING),  # then, oldest
            ],
        )
        LOGGER.debug(f"got backlog entry & marked as pending ({entry.scan_id=})")

        if (
            entry["pending_timestamp"]
            < time.time() - ENV.SCAN_BACKLOG_PENDING_ENTRY_TTL_REVIVE
            # inequality should still be valid if revival time >> O(ms)
        ):
            LOGGER.debug(f"backlog entry ready for revival ({entry.scan_id=})")
        return entry
