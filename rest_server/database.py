"""Database interface for persisted scan data."""

import pymongo.errors
from motor.motor_tornado import MotorClient  # type: ignore
from tornado import web


class SkyDriverDatabaseClient:
    """MotorClient with additional guardrails for SkyDriver things."""

    def __init__(self, motor_client: MotorClient) -> None:
        self._mongo = motor_client


class MetaDatabaseClient(SkyDriverDatabaseClient):
    """Wraps the database for metadata about a scan."""


class ResultsDatabaseClient(SkyDriverDatabaseClient):
    """Wraps the database for the results of a scan."""
