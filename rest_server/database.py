"""Database interface for persisted scan data."""

import dataclasses as dc
from typing import Iterator, TypeVar

import pymongo.errors
from bson.objectid import ObjectId
from motor.motor_tornado import MotorClient  # type: ignore
from tornado import web


class ScanAttrClient:
    """Allows access to a specific attribute of the 'Scan' collection."""

    def __init__(self, motor_client: MotorClient) -> None:
        self._mongo = motor_client


# -----------------------------------------------------------------------------


class EventPseudoClient(ScanAttrClient):
    """Serves as a wrapper for things about an event."""

    async def get_scan_ids(event_id: str) -> Iterator[str]:
        """Search over scans and find all matching event-id."""
        pass


# -----------------------------------------------------------------------------


@dc.dataclass(frozen=True)
class Inflight:
    """Contains a manifest of the scan."""


@dc.dataclass(frozen=True)
class Result:
    """Encompasses the physics results for a scan."""


@dc.dataclass(frozen=True)
class ScanDoc:
    """Encapsulates a unique scan entity."""

    _id: ObjectId
    is_deleted: bool = False
    event_id: str
    inflight: Inflight
    result: Result = None


# -----------------------------------------------------------------------------


T = TypeVar("T")


class ReadWriteActions:
    """Template common read/write actions."""

    async def get(self, scan_id: str) -> T:
        """Get `T` using `scan_id`."""
        return NotImplemented

    async def set(self, scan_id: str, data: T) -> T:
        """Set/add/update `T` at doc matching `scan_id`."""
        return NotImplemented

    async def mark_as_deleted(self, scan_id: str) -> T:
        """Mark `T` at doc matching `scan_id` as deleted."""
        return NotImplemented


# -----------------------------------------------------------------------------


class InflightClient(ScanAttrClient, ReadWriteActions):
    """Wraps the attribute for the metadata of a scan."""

    async def get(self, scan_id: str) -> Inflight:
        """Get `Inflight` using `scan_id`."""
        return Inflight()

    async def set(self, scan_id: str, data: Inflight) -> Inflight:
        """Set/add/update `Inflight` at doc matching `scan_id`."""
        return Inflight()

    async def mark_as_deleted(self, scan_id: str) -> Inflight:
        """Mark `Inflight` at doc matching `scan_id` as deleted."""
        return Inflight()


# -----------------------------------------------------------------------------


class ResultClient(ScanAttrClient, ReadWriteActions):
    """Wraps the attribute for the result of a scan."""

    async def get(self, scan_id: str) -> Result:
        """Get `Result` using `scan_id`."""
        return Result()

    async def set(self, scan_id: str, data: Result) -> Result:
        """Set/add/update `Result` at doc matching `scan_id`."""
        return Result()

    async def mark_as_deleted(self, scan_id: str) -> Result:
        """Mark `Result` at doc matching `scan_id` as deleted."""
        return Result()
