"""The background runner responsible for launching skymap scanner instances."""

import asyncio
import logging
import time

import kubernetes.client  # type: ignore[import-untyped]
from pymongo import AsyncMongoClient
from tenacity import retry, stop_never, wait_fixed
from tornado import web
from wipac_dev_tools.mongo_jsonschema_tools import (
    MongoDoc,
    MongoJSONSchemaValidatedCollection,
)
from wipac_dev_tools.timing_tools import IntervalTimer

from .. import database
from ..config import ENV
from ..database.interface import ScanBacklogHelper
from ..k8s.utils import KubeAPITools

LOGGER = logging.getLogger(__name__)


async def put_on_backlog(
    scan_id: str,
    scan_backlog: MongoJSONSchemaValidatedCollection,
    priority: int,
) -> None:
    """Enqueue k8s job to be started later by the scan launcher."""
    try:
        LOGGER.info(f"putting future scan on backlog ({scan_id=})")
        entry = database.schema.ScanBacklogEntry(
            scan_id=scan_id,
            timestamp=time.time(),
            priority=priority,
        )
        await scan_backlog.insert_one(entry)
    except Exception as e:
        LOGGER.exception(e)
        raise web.HTTPError(
            400,
            log_message="Failed to put future scan on backlog",
        )


async def get_next(
    db: database.SkyDriverMongoValidatedDatabase,
    include_low_priority_scans: bool,
) -> tuple[MongoDoc, MongoDoc, MongoDoc, MongoDoc]:
    """Get the next entry & remove any that have been cancelled."""
    while True:
        # get next up -- raises DocumentNotFoundException if none
        entry = await ScanBacklogHelper.fetch_next_as_pending(
            db.scan_backlog, include_low_priority_scans
        )
        LOGGER.info(
            f"Got backlog entry "
            f"({entry.scan_id=}, {include_low_priority_scans=}, {entry.priority=})"
        )

        if entry.next_attempt > ENV.SCAN_BACKLOG_MAX_ATTEMPTS:
            LOGGER.info(
                f"Backlog entry was already attempted {ENV.SCAN_BACKLOG_MAX_ATTEMPTS} times "
                f"-- backlog entry will now be removed ({entry.scan_id=})"
            )
            await db.scan_backlog.delete_one({"scan_id": entry.scan_id})
            continue

        # check if scan was 'deleted'
        manifest = await db.manifests.find_one({"scan_id": entry.scan_id})
        if manifest.is_deleted:
            LOGGER.info(
                f"Scan is designated for deletion "
                f"-- backlog entry will now be removed ({entry.scan_id=})"
            )
            await db.scan_backlog.delete_one({"scan_id": entry.scan_id})
            continue

        # grab the scan request object--it has other info
        scan_request_obj = await db.scan_requests.find_one(
            {
                "$or": [
                    {"scan_id": manifest.scan_id},
                    {"rescan_ids": manifest.scan_id},  # one in a list
                ]
            }
        )

        # grab the k8s
        doc = await db.skyscan_k8s_jobs.find_one({"scan_id": manifest.scan_id})
        skyscan_k8s_job = doc["skyscan_k8s_job_dict"]

        # all good!
        return entry, manifest, scan_request_obj, skyscan_k8s_job


@retry(
    wait=wait_fixed(ENV.SCAN_BACKLOG_RUNNER_DELAY),
    stop=stop_never,
    before=lambda x: LOGGER.info(f"Started {__name__}."),
    before_sleep=lambda x: LOGGER.info(f"Restarting {__name__} soon..."),
    after=lambda x: LOGGER.info(f"Restarted {__name__}."),
)
async def run(
    mongo_client: AsyncMongoClient,
    k8s_batch_api: kubernetes.client.BatchV1Api,
) -> None:
    """The main loop."""
    db = database.SkyDriverMongoValidatedDatabase(mongo_client, raise_500=False)

    timer_for_any_priority_scans = IntervalTimer(
        ENV.SCAN_BACKLOG_RUNNER_DELAY, f"{LOGGER.name}.timer"
    )
    timer_for_logging = IntervalTimer(
        ENV.SCAN_BACKLOG_RUNNER_DELAY, f"{LOGGER.name}.heartbeat_timer"
    )

    # main loop
    while True:
        await asyncio.sleep(ENV.SCAN_BACKLOG_RUNNER_SHORT_DELAY)
        if timer_for_logging.has_interval_elapsed():
            LOGGER.info("scan launcher is still alive")

        # include low priority scans only when enough time has passed
        include_low_priority_scans = timer_for_any_priority_scans.has_interval_elapsed()

        # get next entry
        try:
            entry, manifest, scan_request_obj, skyscan_k8s_job = await get_next(
                db,
                include_low_priority_scans=include_low_priority_scans,
            )
        except database.mongodc.DocumentNotFoundException:
            # *** EXTREMELY COMMON SCENARIO ***
            if include_low_priority_scans:
                # reset the timer only if this last search included any-priority scans
                timer_for_any_priority_scans.fastforward()
            continue  # there's no scan to start

        LOGGER.info(
            f"Starting Scanner Instance: ({entry.scan_id=}) ({entry.timestamp})"
        )
        # NOTE: the job_obj is enormous, so don't log it

        # start k8s job -- this could be any k8s job (pre- or post-ewms switchover)
        try:
            LOGGER.info(f"Starting K8s job: scan_id={manifest.scan_id}")
            await KubeAPITools.start_job(
                k8s_batch_api,
                skyscan_k8s_job,
                inf_retry_if_denied_by_job_quota=True,
                logger=LOGGER,
            )
        except kubernetes.utils.FailToCreateError as e:
            # *** EXTREMELY RARE SCENARIO ***
            # k8s job (backlog entry) will be revived & restarted in future iteration
            LOGGER.exception(e)
            # fastforward to avoid idling after failure -- treat as "nothing started"
            timer_for_any_priority_scans.fastforward()
            continue  # 'get_next()' has built-in retry logic

        # NOTE: DO NOT ADD ANYMORE ACTIONS THAT CAN POSSIBLY FAIL -- THINK STATELESSNESS

        # remove from backlog now that startup succeeded
        LOGGER.info(f"Scan successfully started: scan_id={manifest.scan_id}")
        await db.scan_backlog.delete_one({"scan_id": entry.scan_id})
        # and mark time on k8s job doc -- used by the scan pod watchdog
        await db.skyscan_k8s_jobs.find_one_and_update(
            {"scan_id": manifest.scan_id},
            {"$set": {"k8s_started_ts": int(time.time())}},
        )

        # NOTE: no need to sleep here (sleep at top of loop), also see `include_low_priority_scans` logic
