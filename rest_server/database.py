"""Database interface for persisted scan data."""

from typing import Iterator

import pymongo.errors
from motor.motor_tornado import MotorClient  # type: ignore
from tornado import web


class SkyDriverDatabaseClient:
    """MotorClient with additional guardrails for SkyDriver things."""

    def __init__(self, motor_client: MotorClient) -> None:
        self._mongo = motor_client


class EventPseudoDatabaseClient(SkyDriverDatabaseClient):
    """Wraps the database for things about an event."""

    async def get_scan_ids(event_id: str) -> Iterator[str]:
        # search over scans and find all matching event-id
        pass


class ReadWriteActions:
    """Template common read/write actions."""

    async def get(self, scan_id: str) -> T:
        return NotImplemented

    async def set(self, scan_id: str) -> T:
        return NotImplemented

    async def mark_as_deleted(self, scan_id: str) -> T:
        return NotImplemented


class InflightDatabaseClient(SkyDriverDatabaseClient, ReadWriteActions):
    """Wraps the database for metadata about a scan."""


class ResultsDatabaseClient(SkyDriverDatabaseClient, ReadWriteActions):
    """Wraps the database for the results of a scan."""
