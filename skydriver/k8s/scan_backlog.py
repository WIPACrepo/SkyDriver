"""The queuing logic for launching skymap scanner instances."""

import asyncio
import logging
import time

import botocore.client  # type: ignore[import-untyped]
import kubernetes.client  # type: ignore[import-untyped]
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from rest_tools.client import RestClient
from tornado import web
from wipac_dev_tools.timing_tools import IntervalTimer

from .utils import KubeAPITools
from .. import database
from ..config import ENV

LOGGER = logging.getLogger(__name__)


async def put_on_backlog(
    scan_id: str,
    scan_backlog: database.interface.ScanBacklogClient,
    priority: int,
) -> None:
    """Enqueue k8s job to be started by job-starter thread."""
    try:
        LOGGER.info(f"enqueuing k8s job for {scan_id=}")
        entry = database.schema.ScanBacklogEntry(
            scan_id=scan_id,
            timestamp=time.time(),
            priority=priority,
        )
        await scan_backlog.insert(entry)
    except Exception as e:
        LOGGER.exception(e)
        raise web.HTTPError(
            400,
            log_message="Failed to enqueue Kubernetes job for Scanner instance",
        )


async def get_next(
    scan_backlog: database.interface.ScanBacklogClient,
    manifests: database.interface.ManifestClient,
    scan_request_client: AsyncIOMotorCollection,  # type: ignore[valid-type]
    skyscan_k8s_job_client: AsyncIOMotorClient,  # type: ignore[valid-type]
    include_low_priority_scans: bool,
) -> tuple[database.schema.ScanBacklogEntry, database.schema.Manifest, dict, dict]:
    """Get the next entry & remove any that have been cancelled."""
    while True:
        # get next up -- raises DocumentNotFoundException if none
        entry = await scan_backlog.fetch_next_as_pending(include_low_priority_scans)
        LOGGER.info(f"Got backlog entry ({entry.scan_id=})")

        if entry.next_attempt > ENV.SCAN_BACKLOG_MAX_ATTEMPTS:
            LOGGER.info(
                f"Backlog entry was already attempted {ENV.SCAN_BACKLOG_MAX_ATTEMPTS} times ({entry.scan_id=})"
            )
            await scan_backlog.remove(entry)
            continue

        # check if scan was 'deleted'
        manifest = await manifests.get(entry.scan_id, incl_del=True)
        if manifest.is_deleted:
            LOGGER.info(f"Backlog entry was removed ({entry.scan_id=})")
            await scan_backlog.remove(entry)
            continue

        # grab the scan request object--it has other info
        scan_request_obj = await scan_request_client.find_one(  # type: ignore[attr-defined]
            {
                "$or": [
                    {"scan_id": manifest.scan_id},
                    {"rescan_ids": manifest.scan_id},  # one in a list
                ]
            }
        )

        # grab the k8s
        doc = await skyscan_k8s_job_client.find_one(  # type: ignore[attr-defined]
            {"scan_id": manifest.scan_id},
        )
        skyscan_k8s_job = doc["skyscan_k8s_job_dict"]

        # all good!
        return entry, manifest, scan_request_obj, skyscan_k8s_job


async def run(
    mongo_client: AsyncIOMotorClient,  # type: ignore[valid-type]
    k8s_batch_api: kubernetes.client.BatchV1Api,
    ewms_rc: RestClient,
    s3_client: botocore.client.BaseClient,
) -> None:
    """Error-handling around the scan backlog runner loop."""
    LOGGER.info("Started scan backlog runner.")

    while True:
        # let's go!
        try:
            await _run(mongo_client, k8s_batch_api, ewms_rc, s3_client)
        except Exception as e:
            LOGGER.exception(e)
            LOGGER.error(
                f"above error stopped the backlogger, "
                f"resuming in {ENV.SCAN_BACKLOG_RUNNER_DELAY} seconds..."
            )

        # wait hopefully log enough that any transient errors are resolved,
        #   like a mongo pod failure and restart
        await asyncio.sleep(ENV.SCAN_BACKLOG_RUNNER_DELAY)
        LOGGER.info("Restarted scan backlog runner.")


async def _run(
    mongo_client: AsyncIOMotorClient,  # type: ignore[valid-type]
    k8s_batch_api: kubernetes.client.BatchV1Api,
    ewms_rc: RestClient,
    s3_client: botocore.client.BaseClient,
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

    timer_main_loop = IntervalTimer(ENV.SCAN_BACKLOG_RUNNER_DELAY, LOGGER)
    timer_logging = IntervalTimer(ENV.SCAN_BACKLOG_RUNNER_DELAY, LOGGER)

    # main loop
    while True:
        await asyncio.sleep(ENV.SCAN_BACKLOG_RUNNER_SHORT_DELAY)
        if timer_logging.has_interval_elapsed():
            LOGGER.info("scan backlog runner is still alive")

        # get next entry
        try:
            entry, manifest, scan_request_obj, skyscan_k8s_job = await get_next(
                backlog_client,
                manifest_client,
                scan_request_client,
                skyscan_k8s_job_client,
                # include low priority scans only when enough time has passed
                include_low_priority_scans=timer_main_loop.has_interval_elapsed(),
            )
        except database.mongodc.DocumentNotFoundException:
            timer_main_loop.fastforward()
            continue  # there's no scan to start

        LOGGER.info(
            f"Starting Scanner Instance: ({entry.scan_id=}) ({entry.timestamp})"
        )
        # NOTE: the job_obj is enormous, so don't log it

        # start k8s job -- this could be any k8s job (pre- or post-ewms switchover)
        try:
            LOGGER.info(f"Starting K8s job: scan_id={manifest.scan_id}")
            KubeAPITools.start_job(k8s_batch_api, skyscan_k8s_job)
        except kubernetes.utils.FailToCreateError as e:
            # k8s job (backlog entry) will be revived & restarted in future iteration
            LOGGER.exception(e)
            timer_main_loop.fastforward()  # nothing was started, so don't wait long
            continue

        # NOTE: DO NOT ADD ANYMORE ACTIONS THAT CAN POSSIBLY FAIL -- THINK STATELESSNESS

        # remove from backlog now that startup succeeded
        LOGGER.info(f"Scan successfully started: scan_id={manifest.scan_id}")
        await backlog_client.remove(entry)
        # TODO: remove k8s job doc?

        # wait so to not overwhelm resources (also, see `sleep()` at top)
        await asyncio.sleep(
            ENV.SCAN_BACKLOG_RUNNER_DELAY - ENV.SCAN_BACKLOG_RUNNER_SHORT_DELAY
        )
