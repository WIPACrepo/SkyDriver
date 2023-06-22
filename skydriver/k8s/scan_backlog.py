"""The queuing logic for launching skymap scanner instances."""


import asyncio
import logging
import pickle
import time

import bson
import kubernetes.client  # type: ignore[import]
from motor.motor_asyncio import AsyncIOMotorClient  # type: ignore[import]

from .. import database
from ..config import ENV
from .utils import KubeAPITools

LOGGER = logging.getLogger(__name__)


async def enqueue(
    scan_id: str,
    job_obj: kubernetes.client.V1Job,
    scan_backlog: database.interface.ScanBacklogClient,
) -> None:
    """Enqueue k8s job to be started by job-starter thread."""
    entry = database.schema.ScanBacklogEntry(
        scan_id=scan_id,
        timestamp=time.time(),
        pickled_k8s_job=bson.Binary(pickle.dumps(job_obj)),
    )
    await scan_backlog.insert(entry)


async def get_next_backlog_entry(
    scan_backlog: database.interface.ScanBacklogClient,
    manifests: database.interface.ManifestClient,
) -> database.schema.ScanBacklogEntry:
    """Get the next entry & remove any that have been cancelled."""
    while True:
        # get next up -- raises DocumentNotFoundException if none
        entry = await scan_backlog.fetch_next_as_pending()
        LOGGER.info(f"Got backlog entry ({entry.scan_id=})")

        # check if scan was aborted (cancelled)
        manifest = await manifests.get(entry.scan_id, incl_del=True)
        if manifest.is_deleted:
            LOGGER.info(f"Backlog entry was aborted ({entry.scan_id=})")
            await scan_backlog.remove(entry)
            continue
        else:
            return entry  # ready to start job


async def startup(
    mongo_client: AsyncIOMotorClient,
    api_instance: kubernetes.client.BatchV1Api,
) -> None:
    """The main loop."""
    LOGGER.info("Started scan backlog runner.")

    manifests = database.interface.ManifestClient(mongo_client)
    scan_backlog = database.interface.ScanBacklogClient(mongo_client)

    first = True  # don't wait for full delay after first starting up (helpful for testing new changes)

    while True:
        if first:
            await asyncio.sleep(ENV.SCAN_BACKLOG_RUNNER_INITIAL_DELAY)
        else:
            await asyncio.sleep(ENV.SCAN_BACKLOG_RUNNER_DELAY)

        # get next entry
        try:
            entry = await get_next_backlog_entry(scan_backlog, manifests)
            first = False
        except database.interface.DocumentNotFoundException:
            continue  # empty queue

        # get k8s job object
        try:
            job_obj = pickle.loads(entry.pickled_k8s_job)
        except Exception as e:
            LOGGER.exception(e)
            continue

        LOGGER.info(
            f"Starting Scanner Instance: ({entry.scan_id=}) ({entry.timestamp}) {job_obj}"
        )

        # start job
        try:
            resp = KubeAPITools.start_job(api_instance, job_obj)
            # job (entry) will be revived & restarted in future iteration
            LOGGER.info(resp)
        except kubernetes.client.exceptions.ApiException as e:
            LOGGER.exception(e)
            continue

        # remove from backlog now that startup succeeded
        await scan_backlog.remove(entry)
