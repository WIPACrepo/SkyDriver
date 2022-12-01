"""Database interface for persisted scan data."""

import dataclasses as dc
from typing import Iterator

import pymongo.errors
from bson.objectid import ObjectId
from motor.motor_tornado import MotorClient  # type: ignore
from tornado import web


class SkyDriverCollectionClient:
    """MotorClient with additional guardrails for SkyDriver things."""

    def __init__(self, motor_client: MotorClient) -> None:
        self._mongo = motor_client


# -----------------------------------------------------------------------------


class EventPseudoCollectionClient(SkyDriverCollectionClient):
    """Serves as a wrapper for things about an event."""

    async def get_scan_ids(event_id: str) -> Iterator[str]:
        """Search over scans and find all matching event-id."""
        pass


# -----------------------------------------------------------------------------


@dc.dataclass(frozen=True)
class MongoDoc:
    """The generic MongoDB document for SkyDriver."""

    _id: ObjectId
    is_deleted: bool


class ReadWriteActions:
    """Template common read/write actions."""

    async def get(self, scan_id: str) -> MongoDoc:
        """Get `MongoDoc` using `scan_id`."""
        return NotImplemented

    async def set(self, scan_id: str, doc: MongoDoc) -> MongoDoc:
        """Set/add/update `MongoDoc` at doc matching `scan_id`."""
        return NotImplemented

    async def mark_as_deleted(self, scan_id: str) -> MongoDoc:
        """Mark `MongoDoc` at doc matching `scan_id` as deleted."""
        return NotImplemented


# -----------------------------------------------------------------------------


@dc.dataclass(frozen=True)
class InflightDoc(MongoDoc):
    """The MongoDB document for the 'Inflight' collection."""

    pass


class InflightCollectionClient(SkyDriverCollectionClient, ReadWriteActions):
    """Wraps the collection for metadata about a scan."""

    async def get(self, scan_id: str) -> InflightDoc:
        """Get `InflightDoc` using `scan_id`."""
        return NotImplemented

    async def set(self, scan_id: str, doc: InflightDoc) -> InflightDoc:
        """Set/add/update `InflightDoc` at doc matching `scan_id`."""
        return NotImplemented

    async def mark_as_deleted(self, scan_id: str) -> InflightDoc:
        """Mark `InflightDoc` at doc matching `scan_id` as deleted."""
        return NotImplemented


# -----------------------------------------------------------------------------


@dc.dataclass(frozen=True)
class ResultsDoc(MongoDoc):
    """The MongoDB document for the 'results' collection."""

    pass


class ResultsCollectionClient(SkyDriverCollectionClient, ReadWriteActions):
    """Wraps the collection for the results of a scan."""

    async def get(self, scan_id: str) -> ResultsDoc:
        """Get `ResultsDoc` using `scan_id`."""
        return NotImplemented

    async def set(self, scan_id: str, doc: ResultsDoc) -> ResultsDoc:
        """Set/add/update `ResultsDoc` at doc matching `scan_id`."""
        return NotImplemented

    async def mark_as_deleted(self, scan_id: str) -> ResultsDoc:
        """Mark `ResultsDoc` at doc matching `scan_id` as deleted."""
        return NotImplemented
