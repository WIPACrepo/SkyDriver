"""The queuing logic for launching skymap scanner instances."""

import asyncio
import logging
import time

import kubernetes.client.V1Job  # type: ignore[import-untyped]
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from rest_tools.client import RestClient
from tornado import web

from .utils import KubeAPITools
from .. import database, ewms
from ..config import ENV

LOGGER = logging.getLogger(__name__)


async def designate_for_startup(
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
        scan_request_obj = await scan_request_client.find_one(
            {"scan_id": manifest.scan_id}
        )

        # grab the k8s
        doc = await skyscan_k8s_job_client.find_one({"scan_id": manifest.scan_id})
        skyscan_k8s_job = doc["skyscan_k8s_job_dict"]

        # all good!
        return entry, manifest, scan_request_obj, skyscan_k8s_job


async def run(
    mongo_client: AsyncIOMotorClient,  # type: ignore[valid-type]
    k8s_batch_api: kubernetes.client.BatchV1Api,
    ewms_rc: RestClient,
) -> None:
    """Error-handling around the scan backlog runner loop."""
    LOGGER.info("Started scan backlog runner.")

    while True:
        # let's go!
        try:
            await _run(mongo_client, k8s_batch_api, ewms_rc)
        except Exception as e:
            LOGGER.exception(e)

        # wait hopefully log enough that any transient errors are resolved,
        #   like a mongo pod failure and restart
        await asyncio.sleep(ENV.SCAN_BACKLOG_RUNNER_DELAY)
        LOGGER.info("Restarted scan backlog runner.")


def _logging_heartbeat(last_log_time: float) -> float:
    if time.time() - last_log_time > ENV.SCAN_BACKLOG_RUNNER_DELAY:
        LOGGER.info("scan backlog runner is still alive")
        return time.time()
    else:
        return last_log_time


class IntervalTimer:
    """A utility class to track time intervals.

    This class allows tracking of elapsed time between actions and provides
    mechanisms to wait until a specified time interval has passed.

    TODO: Move this to dev-tools (copied from TMS).
    """

    def __init__(self, seconds: float, logger: logging.Logger) -> None:
        self.seconds = seconds
        self._last_time = time.time()
        self.logger = logger

    def fastforward(self):
        """Reset the timer so that the next call to `has_interval_elapsed` will return True.

        This effectively skips the current interval and forces the timer to indicate
        that the interval has elapsed on the next check.
        """
        self._last_time = float("-inf")

    async def wait_until_interval(self) -> None:
        """Wait asynchronously until the specified interval has elapsed.

        This method checks the elapsed time every second, allowing cooperative
        multitasking during the wait.
        """
        self.logger.debug(
            f"Waiting until {self.seconds}s has elapsed since the last iteration..."
        )
        while not self.has_interval_elapsed():
            await asyncio.sleep(1)

    def has_interval_elapsed(self) -> bool:
        """Check if the specified time interval has elapsed since the last expiration.

        If the interval has elapsed, the internal timer is reset to the current time.
        """
        diff = time.time() - self._last_time
        if diff >= self.seconds:
            self._last_time = time.time()
            self.logger.debug(
                f"At least {self.seconds}s have elapsed (actually {diff}s)."
            )
            return True
        return False


async def _run(
    mongo_client: AsyncIOMotorClient,  # type: ignore[valid-type]
    k8s_batch_api: kubernetes.client.BatchV1Api,
    ewms_rc: RestClient,
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

    last_log_heartbeat = 0.0  # log every so often, not on every iteration
    long_interval_timer = IntervalTimer(ENV.SCAN_BACKLOG_RUNNER_DELAY, LOGGER)

    while True:
        await asyncio.sleep(ENV.SCAN_BACKLOG_RUNNER_SHORT_DELAY)
        last_log_heartbeat = _logging_heartbeat(last_log_heartbeat)

        # get next entry
        try:
            entry, manifest, scan_request_obj, skyscan_k8s_job = await get_next(
                backlog_client,
                manifest_client,
                scan_request_client,
                skyscan_k8s_job_client,
                # include low priority scans only when enough time has passed
                include_low_priority_scans=long_interval_timer.has_interval_elapsed(),
            )
        except database.mongodc.DocumentNotFoundException:
            long_interval_timer.fastforward()
            continue  # empty queue-

        # request a workflow on EWMS
        try:
            workflow_id = await ewms.request_workflow_on_ewms(
                ewms_rc,
                manifest,
                scan_request_obj,
            )
        except Exception as e:
            LOGGER.exception(e)
            long_interval_timer.fastforward()  # nothing was started, so don't wait long
            continue
        await manifest_client.collection.find_one_and_update(
            {"scan_id": manifest.scan_id},
            {"$set": {"ewms_workflow_id": workflow_id}},
            return_dclass=dict,
        )

        LOGGER.info(
            f"Starting Scanner Instance: ({entry.scan_id=}) ({entry.timestamp})"
        )
        # NOTE: the job_obj is enormous, so don't log it

        # start k8s job -- this could be any k8s job (pre- or post-ewms switchover)
        try:
            resp = KubeAPITools.start_job(k8s_batch_api, skyscan_k8s_job)
            LOGGER.info(resp)
        except kubernetes.client.exceptions.ApiException as e:
            # k8s job (backlog entry) will be revived & restarted in future iteration
            LOGGER.exception(e)
            long_interval_timer.fastforward()  # nothing was started, so don't wait long
            continue

        # remove from backlog now that startup succeeded
        await backlog_client.remove(entry)
        # TODO: remove k8s job doc?
