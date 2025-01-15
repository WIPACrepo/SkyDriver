"""General Mongo utils."""

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING

from ..config import ENV

_DB_NAME = "SkyDriver_DB"
_MANIFEST_COLL_NAME = "Manifests"
_RESULTS_COLL_NAME = "Results"
_SCAN_BACKLOG_COLL_NAME = "ScanBacklog"
_SCAN_REQUEST_COLL_NAME = "ScanRequests"
_I3_EVENT_COLL_NAME = "I3Events"
_SKYSCAN_K8S_JOB_COLL_NAME = "SkyScanK8sJobs"


async def ensure_indexes(motor_client: AsyncIOMotorClient) -> None:  # type: ignore[valid-type]
    """Create indexes in collections.

    Call on server startup.
    """
    # USER SCAN REQUESTS COLL
    await motor_client[_DB_NAME][_SCAN_REQUEST_COLL_NAME].create_index(  # type: ignore[index]
        "scan_id",
        name="scan_id_index",
        unique=True,
    )

    # I3 EVENTS COLL
    await motor_client[_DB_NAME][_I3_EVENT_COLL_NAME].create_index(  # type: ignore[index]
        "i3_event_id",
        name="i3_event_id_index",
        unique=True,
    )

    # SKYSCAN K8S JOB COLL
    await motor_client[_DB_NAME][_SKYSCAN_K8S_JOB_COLL_NAME].create_index(  # type: ignore[index]
        "scan_id",
        name="scan_id_index",
        unique=True,
    )

    # MANIFEST COLL
    await motor_client[_DB_NAME][_MANIFEST_COLL_NAME].create_index(  # type: ignore[index]
        "scan_id",
        name="scan_id_index",
        unique=True,
    )
    await motor_client[_DB_NAME][_MANIFEST_COLL_NAME].create_index(  # type: ignore[index]
        [
            ("event_metadata.event_id", DESCENDING),
            ("event_metadata.run_id", DESCENDING),
        ],
        name="event_run_index",
    )

    # RESULTS COLL
    await motor_client[_DB_NAME][_RESULTS_COLL_NAME].create_index(  # type: ignore[index]
        "scan_id",
        name="scan_id_index",
        unique=True,
    )

    # SCAN BACKLOG COLL
    await motor_client[_DB_NAME][_SCAN_BACKLOG_COLL_NAME].create_index(  # type: ignore[index]
        [("timestamp", ASCENDING)],
        name="timestamp_index",
        unique=False,
    )
    await motor_client[_DB_NAME][_SCAN_BACKLOG_COLL_NAME].create_index(  # type: ignore[index]
        [("priority", DESCENDING)],
        name="priority_index",
        unique=False,
    )
    await motor_client[_DB_NAME][_SCAN_BACKLOG_COLL_NAME].create_index(  # type: ignore[index]
        "scan_id",
        name="scan_id_index",
        unique=True,
    )


async def drop_database(motor_client: AsyncIOMotorClient) -> None:  # type: ignore[valid-type]
    """Drop the database -- only useful during CI testing."""
    if not ENV.CI:
        raise RuntimeError("Cannot drop database if not in testing mode")
    await motor_client.drop_database(_DB_NAME)  # type: ignore[attr-defined]
