"""Database interface for persisted scan data."""

import dataclasses as dc
from typing import Iterator

import pymongo.errors
from motor.motor_tornado import MotorClient, MotorCollection  # type: ignore
from tornado import web


@dc.dataclass(frozen=True)
class Inflight:
    """Contains a manifest of the scan."""


@dc.dataclass(frozen=True)
class Result:
    """Encompasses the physics results for a scan."""


@dc.dataclass(frozen=True)
class ScanDoc:
    """Encapsulates a unique scan entity."""

    uuid: str
    is_deleted: bool = False
    event_id: str
    inflight: Inflight
    result: Result = None


# -----------------------------------------------------------------------------


class DocumentNotFoundError(Exception):
    """Raised when a document is not found."""


_DB_NAME = "SKYDRIVER_DB"
_COLL_NAME = "SCANS"


class ScanCollectionFacade:
    """Allows specific semantic actions on the 'Scan' collection."""

    def __init__(self, motor_client: MotorClient) -> None:
        self.collection: MotorCollection = motor_client[_DB_NAME][_COLL_NAME]

    async def get_doc(self, scan_id: str) -> ScanDoc:
        """Get document by 'scan_id'."""
        query = {"scan_id": scan_id}
        doc = await self.collection.find_one(query)
        if not doc:
            raise DocumentNotFoundError(query)
        return ScanDoc(**doc)


# -----------------------------------------------------------------------------


class EventPseudoClient(ScanCollectionFacade):
    """Serves as a wrapper for things about an event."""

    async def get_scan_ids(self, event_id: str) -> Iterator[str]:
        """Search over scans and find all matching event-id."""
        pass


# -----------------------------------------------------------------------------


class InflightClient(ScanCollectionFacade):
    """Wraps the attribute for the metadata of a scan."""

    async def get(self, scan_id: str) -> Inflight:
        """Get `Inflight` using `scan_id`."""
        doc = await self.get_doc(scan_id)
        return doc.inflight

    async def post(self, data: Inflight, event_id: str) -> Inflight:
        """Create `Inflight` doc."""
        return Inflight()

    async def patch(self, scan_id: str, data: Inflight) -> Inflight:
        """Update `Inflight` at doc matching `scan_id`."""
        return Inflight()

    async def mark_as_deleted(self, scan_id: str) -> Inflight:
        """Mark `Inflight` at doc matching `scan_id` as deleted."""

        self.collection.update({"is_deleted": True})
        return Inflight()


# -----------------------------------------------------------------------------


class ResultClient(ScanCollectionFacade):
    """Wraps the attribute for the result of a scan."""

    async def get(self, scan_id: str) -> Result | None:
        """Get `Result` using `scan_id`."""
        doc = await self.get_doc(scan_id)
        return doc.result

    async def put(self, scan_id: str, data: Result) -> Result:
        """Override `Result` at doc matching `scan_id`."""
        return Result()

    async def mark_as_deleted(self, scan_id: str) -> Result:
        """Mark `Result` at doc matching `scan_id` as deleted."""
        return Result()
