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
    get_scan_request_obj_filter,
    get_scan_state_if_final_result_received,
)
from . import utils
from .. import database
from ..config import ENV
from ..k8s.scanner_instance import EnvVarFactory, SkyScanK8sJobFactory
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

        # only keep those that had transiently killed pod(s)
        for scan_id in copy.deepcopy(scan_ids):
            pods = KubeAPITools.get_pods(
                k8s_core_api,
                SkyScanK8sJobFactory.get_job_name(scan_id),
            )
            if not any(KubeAPITools.pod_transiently_killed(p) for p in pods):
                scan_ids.remove(scan_id)

        # remove any that have rescans (these have already been replaced)
        for scan_id in copy.deepcopy(scan_ids):
            doc = await scan_request_client.find_one(
                get_scan_request_obj_filter(scan_id)
            )
            if not doc:
                # condition should never be met
                LOGGER.error(f"could not find scan request object for {scan_id=}")
                scan_ids.remove(scan_id)  # scan id will come up again in next loop
            elif not doc["rescan_ids"]:
                # scan has never been rescanned -> OK to restart (new rescan)
                continue
            elif doc["rescan_ids"][-1] == scan_id:
                # this scan is the most recent rescan -> OK to restart (new rescan)
                continue
            else:
                # this scan already has a new rescan
                scan_ids.remove(scan_id)

        # restart!
        skyd_rc = RestClient(
            ENV.HERE_URL,
            EnvVarFactory.get_skydriver_rest_auth(),
            logger=LOGGER,
            retries=0,
            timeout=60,
        )
        # submit rescans w/ "abort_first" (also stops ewms workers) -- talk to self
        for scan_id in scan_ids:
            await skyd_rc.request(
                "POST", f"/scan/{scan_id}/actions/rescan", {"abort_first": True}
            )
