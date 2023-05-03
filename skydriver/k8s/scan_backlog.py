"""The queuing logic for launching skymap scanner instances."""


import asyncio
import pickle
import time

import bson
import kubernetes.client  # type: ignore[import]

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


async def startup(
    api_instance: kubernetes.client.BatchV1Api,
    scan_backlog: database.interface.ScanBacklogClient,
) -> None:
    """The main loop."""
    LOGGER.info("Started scan backlog runner.")
    while True:
        await asyncio.sleep(5 * 60)

        # get
        try:
            entry = await scan_backlog.peek()
            job_obj = pickle.loads(entry.pickled_k8s_job)
        except database.interface.DocumentNotFoundException:
            continue

        LOGGER.info(f"Starting Scanner Instance: ({entry.timestamp}) {job_obj}")

        # start
        try:
            resp = KubeAPITools.start_job(api_instance, job_obj)
            LOGGER.info(resp)
        except kubernetes.client.exceptions.ApiException as e:
            LOGGER.exception(e)
            continue

        # remove
        await scan_backlog.remove(entry)
