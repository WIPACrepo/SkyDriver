"""The queuing logic for launching skymap scanner instances."""


import asyncio
import pickle
import time

import bson
import kubernetes.client  # type: ignore[import]

from .config import LOGGER
from .database import interface, schema
from .k8s import KubeAPITools


async def enqueue(
    scan_id: str,
    job_obj: kubernetes.client.V1Job,
    scan_backlog: interface.ScanBacklogClient,
) -> None:
    """Enqueue k8s job to be started by job-starter thread."""
    entry = schema.ScanBacklogEntry(
        scan_id=scan_id,
        is_deleted=False,
        timestamp=time.time(),
        pickled_k8s_job=bson.Binary(pickle.dumps(job_obj)),
    )
    await scan_backlog.insert(entry)


async def loop(
    api_instance: kubernetes.client.BatchV1Api,
    scan_backlog: interface.ScanBacklogClient,
) -> None:
    """The main loop."""
    while True:
        await asyncio.sleep(5 * 60)

        # get
        try:
            entry = await scan_backlog.peek()
            job_obj = pickle.loads(entry.pickled_k8s_job)
        except interface.DocumentNotFoundException:
            continue

        LOGGER.info(f"Starting Scanner Instance: ({entry.timestamp}) {job_obj}")

        # start
        try:
            resp = KubeAPITools.start_job(api_instance, job_obj)
            LOGGER.info(resp)
        except Exception as e:
            LOGGER.error(e)
            continue

        # remove
        await scan_backlog.remove(entry)
