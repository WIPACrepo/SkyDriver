"""The backgfilter runner responsible for checking/restarting scanner k8s pods."""

import asyncio
import logging
import time

import kubernetes.client  # type: ignore[import-untyped]
from pymongo import AsyncMongoClient
from rest_tools.client import RestClient
from tenacity import retry, stop_never, wait_fixed
from wipac_dev_tools.mongo_jsonschema_tools import MongoJSONSchemaValidatedCollection
from wipac_dev_tools.timing_tools import IntervalTimer

from .. import database
from ..config import ENV
from ..k8s.scanner_instance import EnvVarFactory, SkyScanK8sJobFactory
from ..k8s.utils import KubeAPITools
from ..utils import (
    get_scan_request_obj_filter,
    get_scan_state_if_final_result_received,
)

LOGGER = logging.getLogger(__name__)

SCAN_POD_WATCHDOG_RECENCY_WINDOW = 60 * 60  # 1 hour ago

# we want to limit the number of rescan replacements (revivals)
SCAN_POD_WATCHDOG_TOO_MANY_TOO_RECENT_SECS = SCAN_POD_WATCHDOG_RECENCY_WINDOW * 10
SCAN_POD_WATCHDOG_TOO_MANY_TOO_RECENT_N = 3


async def _get_recent_scans(
    skyscan_k8s_jobs: MongoJSONSchemaValidatedCollection,
    min_ago: int,
    max_ago: int,
) -> list[str]:
    scan_ids = []
    async for d in skyscan_k8s_jobs.find_all(
        {
            "k8s_started_ts": {
                "$gte": time.time() - max_ago,
                "$lt": time.time() - min_ago,
            }
        },
        ["scan_id"],
    ):
        scan_ids.append(d["scan_id"])
    return scan_ids


async def _has_scan_been_rescanned(
    scan_id: str,
    scan_requests: MongoJSONSchemaValidatedCollection,
) -> bool:
    sro = await scan_requests.find_one(get_scan_request_obj_filter(scan_id))

    if not sro:
        # condition should never be met -- vacuously true
        LOGGER.error(f"could not find scan request object for {scan_id=}")
        return True
    elif not sro["rescan_ids"]:
        # scan has never been rescanned -> OK to restart (new rescan)
        return False
    elif sro["rescan_ids"][-1] == scan_id:
        # this scan is the most recent -> OK to restart (new rescan)
        return False
    else:
        # this scan already has a new rescan
        return True


async def _has_scan_been_rescanned_too_many_times_too_recently(
    scan_id: str,
    scan_requests: MongoJSONSchemaValidatedCollection,
    manifests: MongoJSONSchemaValidatedCollection,
) -> bool:
    sro = await scan_requests.find_one(get_scan_request_obj_filter(scan_id))
    if not sro:
        # condition should never be met -- vacuously true
        LOGGER.error(f"could not find scan request object for {scan_id=}")
        return True

    all_scan_ids = [sro["scan_id"]] + sro["rescan_ids"]

    # count how many in last x mins
    result = await manifests._collection.count_documents(  # there's no public method for this
        {
            "scan_id": {"$in": all_scan_ids},
            "timestamp": {
                "$gt": time.time() - SCAN_POD_WATCHDOG_TOO_MANY_TOO_RECENT_SECS
            },
        }
    )
    return result > SCAN_POD_WATCHDOG_TOO_MANY_TOO_RECENT_N


async def _request_replacement_rescan(skyd_rc: RestClient, scan_id: str) -> None:
    # submit rescans w/ "abort_first" (also stops ewms workers) -- talk to self
    LOGGER.info(f"requesting rescan for {scan_id=}")
    await skyd_rc.request(
        "POST",
        f"/scan/{scan_id}/actions/rescan",
        {"abort_first": True, "replace_scan": True},
    )


@retry(
    wait=wait_fixed(ENV.SCAN_POD_WATCHDOG_DELAY),
    stop=stop_never,
    before=lambda x: LOGGER.info(f"Started {__name__}."),
    before_sleep=lambda x: LOGGER.info(f"Restarting {__name__} soon..."),
    after=lambda x: LOGGER.info(f"Restarted {__name__}."),
)
async def run(
    mongo_client: AsyncMongoClient,
    k8s_core_api: kubernetes.client.CoreV1Api,  # CoreV1Api(k8s_batch_api.api_client)
) -> None:
    """The main loop."""
    try:  # we're only wrapping so we can add logging for any exception
        await _run(mongo_client, k8s_core_api)
    except Exception as e:
        LOGGER.exception(e)
        raise


async def _run(
    mongo_client: AsyncMongoClient,
    k8s_core_api: kubernetes.client.CoreV1Api,  # CoreV1Api(k8s_batch_api.api_client)
) -> None:
    db = database.SkyDriverMongoValidatedDatabase(mongo_client, raise_500=False)
    skyd_rc = RestClient(  # -- talk to self
        ENV.HERE_URL,
        EnvVarFactory.get_skydriver_rest_auth(),
        logger=LOGGER,
        retries=0,
        timeout=60,
    )

    timer_for_logging = IntervalTimer(
        max(ENV.SCAN_POD_WATCHDOG_DELAY, 10 * 60), f"{LOGGER.name}.heartbeat_timer"
    )

    # main loop
    while True:
        await asyncio.sleep(ENV.SCAN_POD_WATCHDOG_DELAY)
        if timer_for_logging.has_interval_elapsed():
            LOGGER.info("scan pod watchdog is still alive")

        # get list of scans that were k8s started recently
        if not (
            scan_ids := await _get_recent_scans(
                db.skyscan_k8s_jobs,
                ENV.SCAN_POD_WATCHDOG_DELAY - 1,  # at most 1 loop ago (1 sec cushion)
                SCAN_POD_WATCHDOG_RECENCY_WINDOW,
            )
        ):
            continue
        LOGGER.debug(f"filter #0 - recent scans     - {len(scan_ids)} {scan_ids}")

        # remove any of those that have finished -- app logic
        scan_ids = [
            s
            for s in scan_ids
            if not await get_scan_state_if_final_result_received(s, db.results)
        ]
        LOGGER.debug(f"filter #1 - non-finished     - {len(scan_ids)} {scan_ids}")

        # remove any that have rescans (these have already been replaced)
        scan_ids = [
            s
            for s in scan_ids
            if not await _has_scan_been_rescanned(s, db.scan_requests)
        ]
        LOGGER.debug(f"filter #2 - not rescanned yet - {len(scan_ids)} {scan_ids}")

        # only keep those that have not been rescanned too many times to recently
        scan_ids = [
            s
            for s in scan_ids
            if not await _has_scan_been_rescanned_too_many_times_too_recently(
                s, db.scan_requests, db.manifests
            )
        ]
        LOGGER.debug(f"filter #3 - not throttled    - {len(scan_ids)} {scan_ids}")

        # only keep those that had transiently killed pod(s)
        scan_ids = [
            s
            for s in scan_ids
            if KubeAPITools.has_transiently_killed_pod(
                k8s_core_api,
                SkyScanK8sJobFactory.get_job_name(s),
            )
        ]
        LOGGER.debug(f"filter #4 - k8s restartable - {len(scan_ids)} {scan_ids}")

        # restart!
        for scan_id in scan_ids:
            await _request_replacement_rescan(skyd_rc, scan_id)
