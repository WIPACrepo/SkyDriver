"""The background runner responsible for checking/restarting scanner k8s pods."""

import asyncio
import copy
import logging
import time

import kubernetes.client  # type: ignore[import-untyped]
import kubernetes.client  # type: ignore[import-untyped]
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from rest_tools.client import RestClient
from wipac_dev_tools.timing_tools import IntervalTimer

from skydriver.utils import (
    get_scan_state_if_final_result_received,
)
from . import utils
from .. import database
from ..config import ENV
from ..k8s.scanner_instance import SkyScanK8sJobFactory
from ..k8s.utils import KubeAPITools

LOGGER = logging.getLogger(__name__)


@utils.resilient_loop("scan pod watchdog", ENV.SCAN_BACKLOG_RUNNER_DELAY, LOGGER)
async def _run(
    mongo_client: AsyncIOMotorClient,  # type: ignore[valid-type]
    k8s_core_api: kubernetes.client.CoreV1Api,  # CoreV1Api(k8s_batch_api.api_client)
    ewms_rc: RestClient,
) -> None:
    """The (actual) main loop."""
    manifest_client = database.interface.ManifestClient(mongo_client)
    results_client = database.interface.ResultClient(mongo_client)
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
        docs = await skyscan_k8s_job_client.find(
            {
                "k8s_started_ts": {
                    "$gte": time.time() - (60 * 60),  # 1 hour ago
                    "$lt": time.time() - (10 * 60),  # 10 mins ago
                }
            }
        )
        scan_ids = [d["scan_id"] for d in docs]

        # remove any of those that have finished -- app logic
        for scan_id in copy.deepcopy(scan_ids):
            if await get_scan_state_if_final_result_received(scan_id, results_client):
                scan_ids.remove(scan_id)

        # compare list to what's actually running
        for scan_id in copy.deepcopy(scan_ids):
            pods = KubeAPITools.get_pods(
                k8s_core_api, SkyScanK8sJobFactory.get_job_name(scan_id)
            )
            # TODO
            if False:
                scan_ids.remove(scan_id)

        # put those back on the backlog (highest priority) -- what about ewms?
