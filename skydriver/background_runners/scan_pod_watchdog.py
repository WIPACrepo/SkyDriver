"""The background runner responsible for checking/restarting scanner k8s pods."""

import asyncio
import logging

import kubernetes.client  # type: ignore[import-untyped]
import kubernetes.client  # type: ignore[import-untyped]
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from wipac_dev_tools.timing_tools import IntervalTimer

from . import utils
from .. import database
from ..config import ENV

LOGGER = logging.getLogger(__name__)


@utils.resilient_loop("scan pod watchdog", ENV.SCAN_BACKLOG_RUNNER_DELAY, LOGGER)
async def _run(
    mongo_client: AsyncIOMotorClient,  # type: ignore[valid-type]
    k8s_batch_api: kubernetes.client.BatchV1Api,
) -> None:
    """The (actual) main loop."""
    manifest_client = database.interface.ManifestClient(mongo_client)
    backlog_client = database.interface.ScanBacklogClient(mongo_client)
    scan_request_client = (
        AsyncIOMotorCollection(  # in contrast, this one is accessed directly
            mongo_client[database.interface._DB_NAME],  # type: ignore[index]
            database.utils._SCAN_REQUEST_COLL_NAME,
        )
    )
    skyscan_k8s_job_client = (
        AsyncIOMotorCollection(  # in contrast, this one is accessed directly
            mongo_client[database.interface._DB_NAME],  # type: ignore[index]
            database.utils._SKYSCAN_K8S_JOB_COLL_NAME,
        )
    )

    timer_for_logging = IntervalTimer(
        ENV.SCAN_BACKLOG_RUNNER_DELAY, f"{LOGGER.name}.heartbeat_timer"
    )

    # main loop
    while True:
        await asyncio.sleep(ENV.SCAN_BACKLOG_RUNNER_SHORT_DELAY)
        if timer_for_logging.has_interval_elapsed():
            LOGGER.info("scan pod watchdog is still alive")

        # get list of backlog entries (that were deleted) in the last hour (more?)

        # remove any of those that have finished -- app logic

        # compare list to what's actually running

        # put those back on the backlog (highest priority) -- what about ewms?
