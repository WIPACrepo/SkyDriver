"""The queuing logic for launching skymap scanner instances."""

import asyncio
import logging
import pickle
import time

import bson
import kubernetes.client  # type: ignore[import-untyped]
from motor.motor_asyncio import AsyncIOMotorClient
from tornado import web

from .utils import KubeAPITools
from .. import database
from ..config import ENV

LOGGER = logging.getLogger(__name__)


async def designate_for_startup(
    scan_id: str,
    job_obj: kubernetes.client.V1Job,
    scan_backlog: database.interface.ScanBacklogClient,
    priority: int,
) -> None:
    """Enqueue k8s job to be started by job-starter thread."""
    try:
        LOGGER.info(f"enqueuing k8s job for {scan_id=}")
        entry = database.schema.ScanBacklogEntry(
            scan_id=scan_id,
            timestamp=time.time(),
            pickled_k8s_job=bson.Binary(pickle.dumps(job_obj)),
            priority=priority,
        )
        await scan_backlog.insert(entry)
    except Exception as e:
        LOGGER.exception(e)
        raise web.HTTPError(
            400,
            log_message="Failed to enqueue Kubernetes job for Scanner instance",
        )


async def get_next_backlog_entry(
    scan_backlog: database.interface.ScanBacklogClient,
    manifests: database.interface.ManifestClient,
) -> database.schema.ScanBacklogEntry:
    """Get the next entry & remove any that have been cancelled."""
    while True:
        # get next up -- raises DocumentNotFoundException if none
        entry = await scan_backlog.fetch_next_as_pending()
        LOGGER.info(f"Got backlog entry ({entry.scan_id=})")

        if entry.next_attempt > ENV.SCAN_BACKLOG_MAX_ATTEMPTS:
            LOGGER.info(
                f"Backlog entry was already attempted {ENV.SCAN_BACKLOG_MAX_ATTEMPTS} times ({entry.scan_id=})"
            )
            await scan_backlog.remove(entry)
            continue

        # check if scan was aborted (cancelled)
        manifest = await manifests.get(entry.scan_id, incl_del=True)
        if manifest.is_deleted:
            LOGGER.info(f"Backlog entry was aborted ({entry.scan_id=})")
            await scan_backlog.remove(entry)
            continue

        # all good!
        return entry  # ready to start job


async def run(
    mongo_client: AsyncIOMotorClient,  # type: ignore[valid-type]
    k8s_batch_api: kubernetes.client.BatchV1Api,
) -> None:
    """Error-handling around the scan backlog runner loop."""
    LOGGER.info("Started scan backlog runner.")

    while True:
        # let's go!
        try:
            await _run(mongo_client, k8s_batch_api)
        except Exception as e:
            LOGGER.exception(e)

        # wait hopefully log enough that any transient errors are resolved,
        #   like a mongo pod failure and restart
        await asyncio.sleep(ENV.SCAN_BACKLOG_RUNNER_DELAY)
        LOGGER.info("Restarted scan backlog runner.")


async def _run(
    mongo_client: AsyncIOMotorClient,  # type: ignore[valid-type]
    k8s_batch_api: kubernetes.client.BatchV1Api,
) -> None:
    """The (actual) main loop."""
    manifests = database.interface.ManifestClient(mongo_client)
    scan_backlog = database.interface.ScanBacklogClient(mongo_client)

    short_sleep = True  # don't wait for full delay after first starting up (helpful for testing new changes)

    last_log_time = 0.0  # keep track of last time a log was made so we're not annoying

    while True:
        if short_sleep:
            await asyncio.sleep(ENV.SCAN_BACKLOG_RUNNER_SHORT_DELAY)
        else:
            await asyncio.sleep(ENV.SCAN_BACKLOG_RUNNER_DELAY)

        # like a heartbeat for the logs
        if time.time() - last_log_time > ENV.SCAN_BACKLOG_RUNNER_DELAY:
            LOGGER.info("scan backlog runner is still alive")
            last_log_time = time.time()

        # get next entry
        try:
            entry = await get_next_backlog_entry(scan_backlog, manifests)
            short_sleep = False
        except database.mongodc.DocumentNotFoundException:
            if not short_sleep:  # don't log too often
                LOGGER.debug("no backlog entry found")
            short_sleep = True
            continue  # empty queue

        # get k8s job object
        try:
            job_obj = pickle.loads(entry.pickled_k8s_job)
        except Exception as e:
            LOGGER.exception(e)
            short_sleep = True  # don't wait long b/c nothing was started
            continue

        LOGGER.info(
            f"Starting Scanner Instance: ({entry.scan_id=}) ({entry.timestamp})"
        )
        # NOTE: the job_obj is enormous, so don't log it

        # start job
        try:
            resp = KubeAPITools.start_job(k8s_batch_api, job_obj)
            LOGGER.info(resp)
        except kubernetes.client.exceptions.ApiException as e:
            # job (entry) will be revived & restarted in future iteration
            LOGGER.exception(e)
            short_sleep = True  # don't wait long b/c nothing was started
            continue

        # remove from backlog now that startup succeeded
        await scan_backlog.remove(entry)
