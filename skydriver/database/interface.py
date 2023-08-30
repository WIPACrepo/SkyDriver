"""Database interface for persisted scan data."""

import dataclasses as dc
import logging
import time
from typing import Any, AsyncIterator

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, ReturnDocument
from tornado import web

from ..config import ENV
from . import mongodc, schema
from .utils import (
    _DB_NAME,
    _MANIFEST_COLL_NAME,
    _RESULTS_COLL_NAME,
    _SCAN_BACKLOG_COLL_NAME,
)

LOGGER = logging.getLogger(__name__)


# -----------------------------------------------------------------------------


class ManifestClient:
    """Wraps the attribute for the metadata of a scan."""

    def __init__(self, motor_client: AsyncIOMotorClient) -> None:  # type: ignore[valid-type]
        self.collection = mongodc.MotorDataclassCollection(
            motor_client[_DB_NAME],  # type: ignore[index]
            _MANIFEST_COLL_NAME,
        )

    async def get(self, scan_id: str, incl_del: bool) -> schema.Manifest:
        """Get `schema.Manifest` using `scan_id`."""
        LOGGER.debug(f"getting manifest for {scan_id=}")

        query: dict[str, Any] = {"scan_id": scan_id}
        if not incl_del:  # if true, we don't care what 'is_deleted' value is
            query["is_deleted"] = False

        try:
            manifest = await self.collection.find_one(
                query,
                return_dclass=schema.Manifest,
            )
        except mongodc.DocumentNotFoundException as e:
            raise web.HTTPError(
                404,
                log_message=f"Document Not Found: {self.collection.name} document ({query})",
            ) from e
        return manifest

    async def post(
        self,
        event_i3live_json_dict: schema.StrDict,
        scan_id: str,
        scanner_server_args: str,
        tms_args_list: list[str],
        env_vars: dict[str, Any],
    ) -> schema.Manifest:
        """Create `schema.Manifest` doc."""
        LOGGER.debug("creating new manifest")

        # validate
        manifest = schema.Manifest(
            scan_id=scan_id,
            is_deleted=False,
            event_i3live_json_dict=event_i3live_json_dict,
            scanner_server_args=scanner_server_args,
            tms_args=tms_args_list,
            env_vars=env_vars,
        )

        # db
        try:
            manifest = await self.collection.find_one_and_update(
                {"scan_id": manifest.scan_id},
                {"$set": dc.asdict(manifest)},
                return_dclass=schema.Manifest,
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
        except mongodc.DocumentNotFoundException as e:
            raise web.HTTPError(
                500,
                log_message=f"Failed to post {self.collection.name} document ({scan_id})",
            ) from e

        return manifest

    async def patch(
        self,
        scan_id: str,
        progress: schema.Progress | None = None,
        event_metadata: schema.EventMetadata | None = None,
        scan_metadata: schema.StrDict | None = None,
        cluster: schema.Cluster | None = None,
        complete: bool | None = None,
    ) -> schema.Manifest:
        """Update `progress` at doc matching `scan_id`."""
        LOGGER.debug(f"patching manifest for {scan_id=}")

        if not (
            progress
            or event_metadata
            or scan_metadata
            or cluster
            or complete is not None  # True/False is ok
        ):
            LOGGER.debug(f"nothing to patch for manifest ({scan_id=})")
            return await self.get(scan_id, incl_del=True)

        upserting: schema.StrDict = {}

        # Store/validate: event_metadata & scan_metadata
        # NOTE: in theory there's a race condition (get+upsert), but it's set-once-only, so it's OK
        in_db = await self.get(scan_id, incl_del=True)
        # event_metadata
        if not event_metadata:
            pass  # don't put in DB
        elif not in_db.event_metadata:
            upserting["event_metadata"] = event_metadata
        elif in_db.event_metadata != event_metadata:
            msg = "Cannot change an existing event_metadata"
            raise web.HTTPError(
                400,
                log_message=msg + f" for {scan_id=}",
                reason=msg,
            )
        # scan_metadata
        if not scan_metadata:
            pass  # don't put in DB
        elif not in_db.scan_metadata:
            upserting["scan_metadata"] = scan_metadata
        elif in_db.scan_metadata != scan_metadata:
            msg = "Cannot change an existing scan_metadata"
            raise web.HTTPError(
                400,
                log_message=msg + f" for {scan_id=}",
                reason=msg,
            )

        # cluster / clusters
        if not cluster:
            pass  # don't put in DB
        else:
            upserting["clusters"] = in_db.clusters + [cluster]

        # complete
        if complete is not None:
            upserting["complete"] = complete

        # progress
        if progress:
            upserting["progress"] = progress

        # validate
        if not upserting:  # did we actually update anything?
            LOGGER.debug(f"nothing to patch for manifest ({scan_id=})")
            return in_db
        try:
            upserting = mongodc.typecheck_as_dc_fields(upserting, schema.Manifest)
        except TypeError as e:
            raise web.HTTPError(
                422,
                log_message=str(e),
                reason=str(e),
            )

        # db
        LOGGER.debug(f"patching manifest for {scan_id=} with {upserting=}")
        try:
            manifest = await self.collection.find_one_and_update(
                {"scan_id": scan_id},
                {"$set": upserting},
                upsert=True,
                return_document=ReturnDocument.AFTER,
                return_dclass=schema.Manifest,
            )
        except mongodc.DocumentNotFoundException as e:
            raise web.HTTPError(
                500,
                log_message=f"Failed to patch {self.collection.name} document ({scan_id})",
            ) from e

        return manifest

    async def mark_as_deleted(self, scan_id: str) -> schema.Manifest:
        """Mark `schema.Manifest` at doc matching `scan_id` as deleted."""
        LOGGER.debug(f"marking manifest as deleted for {scan_id=}")

        try:
            manifest = await self.collection.find_one_and_update(
                {"scan_id": scan_id},
                {"$set": {"is_deleted": True}},
                upsert=True,
                return_document=ReturnDocument.AFTER,
                return_dclass=schema.Manifest,
            )
        except mongodc.DocumentNotFoundException as e:
            raise web.HTTPError(
                500,
                log_message=f"Failed to mark_as_deleted {self.collection.name} document ({scan_id})",
            ) from e

        return manifest

    async def find_all(
        self,
        run_id: int,
        event_id: int,
        is_real_event: bool,
        incl_del: bool,
    ) -> AsyncIterator[schema.Manifest]:
        """Search over scans and find all matching runevent."""
        LOGGER.debug(
            f"finding: scans for {(run_id, event_id, is_real_event)=} ({incl_del=})"
        )

        # skip the dataclass-casting b/c we're just returning a str
        query = {
            "event_metadata.event_id": event_id,
            "event_metadata.run_id": run_id,
            "event_metadata.is_real_event": is_real_event,
            # NOTE: not searching for mjd
        }
        async for manifests in self.collection.find(
            query, return_dclass=schema.Manifest
        ):
            if not incl_del and manifests.is_deleted:
                continue
            LOGGER.debug(
                f"found: {manifests.scan_id=} for {(run_id, event_id, is_real_event)=} ({incl_del=})"
            )
            yield manifests


# -----------------------------------------------------------------------------


class ResultClient:
    """Wraps the attribute for the result of a scan."""

    def __init__(self, motor_client: AsyncIOMotorClient) -> None:  # type: ignore[valid-type]
        self.collection: mongodc.MotorDataclassCollection = mongodc.MotorDataclassCollection(
            motor_client[_DB_NAME], _RESULTS_COLL_NAME  # type: ignore[index]
        )

    async def get(self, scan_id: str) -> schema.Result:
        """Get `schema.Result` using `scan_id`."""
        LOGGER.debug(f"getting result for {scan_id=}")
        try:
            result = await self.collection.find_one(
                {"scan_id": scan_id},
                return_dclass=schema.Result,
            )
        except mongodc.DocumentNotFoundException as e:
            raise web.HTTPError(
                404,
                log_message=f"Document Not Found: {self.collection.name} document ({scan_id=})",
            ) from e
        return result

    async def put(
        self, scan_id: str, skyscan_result: schema.StrDict, is_final: bool
    ) -> schema.Result:
        """Override `schema.Result` at doc matching `scan_id`."""
        LOGGER.debug(f"overriding result for {scan_id=} {is_final=}")

        # validate
        if not skyscan_result:
            msg = f"Attempted to add result with an empty object ({skyscan_result})"
            raise web.HTTPError(
                422,
                log_message=msg + f" for {scan_id=}",
                reason=msg,
            )
        result = schema.Result(scan_id, skyscan_result, is_final)  # validates data

        # db
        try:
            result = await self.collection.find_one_and_update(
                {"scan_id": result.scan_id},
                {"$set": dc.asdict(result)},
                upsert=True,
                return_document=ReturnDocument.AFTER,
                return_dclass=schema.Result,
            )
        except mongodc.DocumentNotFoundException as e:
            raise web.HTTPError(
                500,
                log_message=f"Failed to put {self.collection.name} document ({scan_id})",
            ) from e

        return result


# -----------------------------------------------------------------------------


class ScanBacklogClient:
    """Wraps the attribute for the result of a scan."""

    def __init__(self, motor_client: AsyncIOMotorClient) -> None:  # type: ignore[valid-type]
        self.collection: mongodc.MotorDataclassCollection = mongodc.MotorDataclassCollection(
            motor_client[_DB_NAME], _SCAN_BACKLOG_COLL_NAME  # type: ignore[index]
        )

    async def fetch_next_as_pending(self) -> schema.ScanBacklogEntry:
        """Fetch the next ready entry and mark as pending.

        This for when the container is restarted (process is killed).
        """
        LOGGER.debug("fetching & marking top backlog entry as a pending...")

        # atomically find & update
        entry = await self.collection.find_one_and_update(
            {
                # get entries that have never been pending (0.0) and/or
                # entries that have been pending for too long (parent
                # process may have died) -- younger pending entries may
                # still be in flight by other processes)
                "pending_timestamp": {
                    "$lt": time.time() - ENV.SCAN_BACKLOG_PENDING_ENTRY_TTL_REVIVE
                }
            },
            {"$set": {"pending_timestamp": time.time()}},
            sort=[("timestamp", ASCENDING)],
            return_document=ReturnDocument.AFTER,
            return_dclass=schema.ScanBacklogEntry,
        )
        LOGGER.debug(f"got backlog entry & marked as pending ({entry.scan_id=})")

        if (
            entry.pending_timestamp
            < time.time() - ENV.SCAN_BACKLOG_PENDING_ENTRY_TTL_REVIVE
            # inequality should still be valid if revival time >> O(ms)
        ):
            LOGGER.debug(f"backlog entry ready for revival ({entry.scan_id=})")
        return entry

    async def remove(self, entry: schema.ScanBacklogEntry) -> schema.ScanBacklogEntry:
        """Remove entry, `schema.ScanBacklogEntry`."""
        LOGGER.debug("removing ScanBacklogEntry")
        res = await self.collection.delete_one({"scan_id": entry.scan_id})
        LOGGER.debug(f"delete_one result: {res}")
        return entry

    async def insert(self, entry: schema.ScanBacklogEntry) -> None:
        """Insert entry, `schema.ScanBacklogEntry`."""
        LOGGER.debug(f"inserting {entry=}")
        doc = dc.asdict(entry)
        res = await self.collection.insert_one(doc)
        LOGGER.debug(f"insert result: {res}")
        LOGGER.debug(f"Inserted backlog entry for {entry.scan_id=}")

    async def get_all(self) -> AsyncIterator[dict]:
        """Get all entries in backlog.

        Doesn't include all fields.
        """
        LOGGER.debug("getting all entries in backlog")
        async for entry in self.collection.find(
            {},
            {"_id": False, "pickled_k8s_job": False},
            sort=[("timestamp", ASCENDING)],
            return_dclass=dict,
        ):
            yield entry

    async def is_in_backlog(self, scan_id: str) -> bool:
        """Return whether the scan id is in the backlog."""
        LOGGER.debug(f"looking for {scan_id} in backlog")
        async for _ in self.collection.find(
            {"scan_id": scan_id},
            return_dclass=schema.ScanBacklogEntry,
        ):
            return True
        return False
