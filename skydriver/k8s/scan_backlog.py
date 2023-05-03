"""The queuing logic for launching skymap scanner instances."""


import asyncio
import pickle
import time

import bson
import kubernetes.client  # type: ignore[import]
from motor.motor_asyncio import AsyncIOMotorClient  # type: ignore[import]

from .. import database
from ..config import LOGGER
from .utils import KubeAPITools


async def enqueue(
    scan_id: str,
    job_obj: kubernetes.client.V1Job,
    scan_backlog: database.interface.ScanBacklogClient,
) -> None:
    """Enqueue k8s job to be started by job-starter thread."""
    entry = database.schema.ScanBacklogEntry(
        scan_id=scan_id,
        is_deleted=False,
        timestamp=time.time(),
        pickled_k8s_job=bson.Binary(pickle.dumps(job_obj)),
    )
    await scan_backlog.insert(entry)


async def get_next_job(
    scan_backlog: database.interface.ScanBacklogClient,
    manifests: database.interface.ManifestClient,
) -> database.schema.ScanBacklogEntry:
    """Get the next job & remove any jobs that have been cancelled."""
    while True:
        entry = await scan_backlog.peek()  # raises DocumentNotFoundException

        # check if scan was aborted (cancelled)
        manifest = await manifests.get(entry.scan_id, incl_del=True)
        if manifest.is_deleted:
            await scan_backlog.remove(entry)
            continue
        else:
            return entry  # ready to start job


async def startup(
    api_instance: kubernetes.client.BatchV1Api,
    mongo_client: AsyncIOMotorClient,
) -> None:
    """The main loop."""
    LOGGER.info("Started scan backlog runner.")

    manifests = database.interface.ManifestClient(mongo_client)
    scan_backlog = database.interface.ScanBacklogClient(mongo_client)

    while True:
        await asyncio.sleep(5 * 60)

        # get next job
        try:
            entry = await get_next_job(scan_backlog, manifests)
        except database.interface.DocumentNotFoundException:
            continue  # empty queue
        job_obj = pickle.loads(entry.pickled_k8s_job)

        LOGGER.info(f"Starting Scanner Instance: ({entry.timestamp}) {job_obj}")

        # start job
        try:
            resp = KubeAPITools.start_job(api_instance, job_obj)
            LOGGER.info(resp)
        except kubernetes.client.exceptions.ApiException as e:
            LOGGER.exception(e)
            continue

        # remove from backlog
        await scan_backlog.remove(entry)
