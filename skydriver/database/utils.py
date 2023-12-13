"""General Mongo utils."""


from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING

from ..config import ENV

_DB_NAME = "SkyDriver_DB"
_MANIFEST_COLL_NAME = "Manifests"
_RESULTS_COLL_NAME = "Results"
_SCAN_BACKLOG_COLL_NAME = "ScanBacklog"


async def ensure_indexes(motor_client: AsyncIOMotorClient) -> None:  # type: ignore[valid-type]
    """Create indexes in collections.

    Call on server startup.
    """
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


async def drop_collections(motor_client: AsyncIOMotorClient) -> None:  # type: ignore[valid-type]
    """Drop the "regular" collections -- most useful for testing."""
    if not ENV.CI_TEST:
        raise RuntimeError("Cannot drop collections if not in testing mode")
    await motor_client[_DB_NAME][_MANIFEST_COLL_NAME].drop()  # type: ignore[index]
    await motor_client[_DB_NAME][_RESULTS_COLL_NAME].drop()  # type: ignore[index]
    await motor_client[_DB_NAME][_SCAN_BACKLOG_COLL_NAME].drop()  # type: ignore[index]
