"""Database interface for persisted scan data."""

import dataclasses as dc
import uuid
from typing import Iterator

import pymongo.errors
from dacite import from_dict
from motor.motor_tornado import MotorClient, MotorCollection  # type: ignore
from tornado import web


@dc.dataclass(frozen=True)
class Manifest:
    """Contains a manifest of the scan."""


@dc.dataclass(frozen=True)
class Result:
    """Encompasses the physics results for a scan."""


@dc.dataclass(frozen=True)
class ScanDoc:
    """Encapsulates a unique scan entity."""

    uuid: str
    event_id: str
    manifest: Manifest
    progress: dict = None
    result: Result = None
    is_deleted: bool = False


# -----------------------------------------------------------------------------


class DocumentNotFoundError(Exception):
    """Raised when a document is not found."""


_DB_NAME = "SKYDRIVER_DB"
_COLL_NAME = "SCANS"


class ScanCollectionFacade:
    """Allows specific semantic actions on the 'Scan' collection."""

    def __init__(self, motor_client: MotorClient) -> None:
        self._coll: MotorCollection = motor_client[_DB_NAME][_COLL_NAME]

    async def get_scandoc(self, scan_id: str) -> ScanDoc:
        """Get document by 'scan_id'."""
        query = {"scan_id": scan_id}
        doc = await self._coll.find_one(query)
        if not doc:
            raise DocumentNotFoundError(query)
        return from_dict(ScanDoc, doc)

    async def upsert_scandoc(self, doc: ScanDoc) -> ScanDoc:
        """Insert/update the doc."""
        res = await self._coll.replace_one(
            {"scan_id": doc.scan_id},
            dc.asdict(doc),
            upsert=True,
        )
        if not res["modifiedCount"]:
            raise web.HTTPError(
                500,
                reason=f"Failed to insert scan document ({doc.scan_id})",
            )
        return doc


# -----------------------------------------------------------------------------


class EventPseudoClient(ScanCollectionFacade):
    """Serves as a wrapper for things about an event."""

    async def get_scan_ids(self, event_id: str, include_deleted: bool) -> Iterator[str]:
        """Search over scans and find all matching event-id."""

        # we're going to skip the dataclass (ScanDoc) stuff for right now
        async for doc in self._coll.find({"event_id": event_id}):
            if not include_deleted and doc["is_deleted"]:
                continue
            yield doc["scan_id"]


# -----------------------------------------------------------------------------


class InflightClient(ScanCollectionFacade):
    """Wraps the attribute for the metadata of a scan."""

    async def get(self, scan_id: str) -> Manifest:
        """Get `Manifest` using `scan_id`."""
        doc = await self.get_scandoc(scan_id)
        return doc.manifest

    async def post(self, manifest: Manifest, event_id: str) -> Manifest:
        """Create `Manifest` doc."""
        doc = ScanDoc(
            uuid.uuid4().hex,
            event_id,
            manifest,
        )
        await self.upsert_scandoc(doc)
        return doc.manifest

    async def patch(self, scan_id: str, progress: dict) -> dict:
        """Update `progress` at doc matching `scan_id`."""
        doc = await self.get_scandoc(scan_id)
        doc.progress = progress
        await self.upsert_scandoc(doc)
        return doc.progress

    async def mark_as_deleted(self, scan_id: str) -> Manifest:
        """Mark `Manifest` at doc matching `scan_id` as deleted."""
        doc = await self.get_scandoc(scan_id)
        doc.is_deleted = True
        await self.upsert_scandoc(doc)
        return doc.manifest


# -----------------------------------------------------------------------------


class ResultClient(ScanCollectionFacade):
    """Wraps the attribute for the result of a scan."""

    async def get(self, scan_id: str) -> Result | None:
        """Get `Result` using `scan_id`."""
        doc = await self.get_scandoc(scan_id)
        return doc.result

    async def put(self, scan_id: str, data: Result) -> Result:
        """Override `Result` at doc matching `scan_id`."""
        return Result()

    async def mark_as_deleted(self, scan_id: str) -> Result:
        """Mark `Result` at doc matching `scan_id` as deleted."""
        return Result()
