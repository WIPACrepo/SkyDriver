"""The background runner responsible for checking/restarting scanner k8s pods."""

import asyncio
import logging
import time

import kubernetes.client  # type: ignore[import-untyped]
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from rest_tools.client import RestClient
from wipac_dev_tools.timing_tools import IntervalTimer

from . import utils
from .. import database
from ..config import ENV
from ..k8s.scanner_instance import EnvVarFactory, SkyScanK8sJobFactory
from ..k8s.utils import KubeAPITools
from ..utils import (
    get_scan_request_obj_filter,
    get_scan_state_if_final_result_received,
)

LOGGER = logging.getLogger(__name__)


async def _get_recent_scans(
    skyscan_k8s_job_client: AsyncIOMotorCollection,  # type: ignore[valid-type]
) -> list[str]:
    scan_ids = []
    async for d in skyscan_k8s_job_client.find(  # type: ignore[attr-defined]
        {
            "k8s_started_ts": {
                "$gte": time.time() - (60 * 60),  # 1 hour ago
                "$lt": time.time() - (10 * 60),  # 10 mins ago
            }
        }
    ):
        scan_ids.append(d["scan_id"])
    return scan_ids


async def _has_scan_been_rescanned(
    scan_id: str,
    scan_request_client: AsyncIOMotorCollection,  # type: ignore[valid-type]
) -> bool:
    doc = await scan_request_client.find_one(  # type: ignore[attr-defined]
        get_scan_request_obj_filter(scan_id)
    )

    if not doc:
        # condition should never be met -- vacuously true
        LOGGER.error(f"could not find scan request object for {scan_id=}")
        return True
    elif not doc["rescan_ids"]:
        # scan has never been rescanned -> OK to restart (new rescan)
        return False
    elif doc["rescan_ids"][-1] == scan_id:
        # this scan is the most recent -> OK to restart (new rescan)
        return False
    else:
        # this scan already has a new rescan
        return True


@utils.resilient_loop("scan pod watchdog", ENV.SCAN_BACKLOG_RUNNER_DELAY, LOGGER)
async def _run(
    mongo_client: AsyncIOMotorClient,  # type: ignore[valid-type]
    k8s_core_api: kubernetes.client.CoreV1Api,  # CoreV1Api(k8s_batch_api.api_client)
) -> None:
    """The (actual) main loop."""
    results_client = database.interface.ResultClient(mongo_client)
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
    skyd_rc = RestClient(  # -- talk to self
        ENV.HERE_URL,
        EnvVarFactory.get_skydriver_rest_auth(),
        logger=LOGGER,
        retries=0,
        timeout=60,
    )

    timer_for_logging = IntervalTimer(
        ENV.SCAN_BACKLOG_RUNNER_DELAY, f"{LOGGER.name}.heartbeat_timer"
    )

    # main loop
    while True:
        await asyncio.sleep(ENV.SCAN_BACKLOG_RUNNER_SHORT_DELAY)
        if timer_for_logging.has_interval_elapsed():
            LOGGER.info("scan pod watchdog is still alive")

        # get list of scans that were k8s started recently
        if not (scan_ids := await _get_recent_scans(skyscan_k8s_job_client)):
            continue

        LOGGER.debug(f"round I: candidates = {len(scan_ids)} {scan_ids}")

        # remove any of those that have finished -- app logic
        scan_ids = [
            s
            for s in scan_ids
            if not await get_scan_state_if_final_result_received(s, results_client)
        ]

        LOGGER.debug(f"round II: candidates = {len(scan_ids)} {scan_ids}")

        # only keep those that had transiently killed pod(s)
        scan_ids = [
            s
            for s in scan_ids
            if KubeAPITools.has_transiently_killed_pod(
                k8s_core_api,
                SkyScanK8sJobFactory.get_job_name(s),
            )
        ]

        LOGGER.debug(f"round III: candidates = {len(scan_ids)} {scan_ids}")

        # remove any that have rescans (these have already been replaced)
        scan_ids = [
            s
            for s in scan_ids
            if not await _has_scan_been_rescanned(s, scan_request_client)
        ]

        LOGGER.debug(f"watchdog finalists = {len(scan_ids)} {scan_ids}")

        # restart!
        # submit rescans w/ "abort_first" (also stops ewms workers) -- talk to self
        for scan_id in scan_ids:
            LOGGER.info(f"requesting rescan for {scan_id=}")
            await skyd_rc.request(
                "POST",
                f"/scan/{scan_id}/actions/rescan",
                {"abort_first": True, "replace_scan": True},
            )
