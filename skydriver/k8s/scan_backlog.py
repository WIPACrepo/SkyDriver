"""The queuing logic for launching skymap scanner instances."""

import asyncio
import logging
import pickle
import time

import bson
import kubernetes.client  # type: ignore[import-untyped]
from motor.motor_asyncio import AsyncIOMotorClient
from rest_tools.client import RestClient
from tornado import web

from .utils import KubeAPITools
from .. import database, ewms
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
    include_low_priority_scans: bool,
) -> tuple[database.schema.ScanBacklogEntry, database.schema.Manifest]:
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

        # check if scan was aborted (cancelled)
        manifest = await manifests.get(entry.scan_id, incl_del=True)
        if manifest.is_deleted:
            LOGGER.info(f"Backlog entry was aborted ({entry.scan_id=})")
            await scan_backlog.remove(entry)
            continue

        # all good!
        return entry, manifest  # ready to start job


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

    last_log_heartbeat = 0.0  # log every so often, not on every iteration
    long_interval_timer = IntervalTimer(ENV.SCAN_BACKLOG_RUNNER_DELAY, LOGGER)

    while True:
        await asyncio.sleep(ENV.SCAN_BACKLOG_RUNNER_SHORT_DELAY)
        last_log_heartbeat = _logging_heartbeat(last_log_heartbeat)

        # get next entry
        try:
            entry, manifest = await get_next_backlog_entry(
                backlog_client,
                manifest_client,
                # include low priority scans only when enough time has passed
                include_low_priority_scans=long_interval_timer.has_interval_elapsed(),
            )
        except database.mongodc.DocumentNotFoundException:
            long_interval_timer.fastforward()
            continue  # empty queue

        # request a workflow on EWMS
        if isinstance(manifest.ewms_task, database.schema.EWMSRequestInfo):
            try:
                workflow_id = await ewms.request_workflow_on_ewms(ewms_rc, manifest)
            except Exception as e:
                LOGGER.exception(e)
                long_interval_timer.fastforward()  # nothing was started, so don't wait long
                continue
            await manifest_client.collection.find_one_and_update(
                {"scan_id": manifest.scan_id},
                {"$set": {"ewms_task.workflow_id": workflow_id}},
            )

        # TODO: Start K8s Job

        # get k8s job object
        try:
            job_obj = pickle.loads(entry.pickled_k8s_job)
        except Exception as e:
            LOGGER.exception(e)
            long_interval_timer.fastforward()  # nothing was started, so don't wait long
            continue

        LOGGER.info(
            f"Starting Scanner Instance: ({entry.scan_id=}) ({entry.timestamp})"
        )
        # NOTE: the job_obj is enormous, so don't log it

        # start k8s job
        try:
            resp = KubeAPITools.start_job(k8s_batch_api, job_obj)
            LOGGER.info(resp)
        except kubernetes.client.exceptions.ApiException as e:
            # k8s job (backlog entry) will be revived & restarted in future iteration
            LOGGER.exception(e)
            long_interval_timer.fastforward()  # nothing was started, so don't wait long
            continue

        # remove from backlog now that startup succeeded
        await backlog_client.remove(entry)
