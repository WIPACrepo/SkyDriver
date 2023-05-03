"""The queuing logic for launching skymap scanner instances."""


import asyncio
import time

import kubernetes.client  # type: ignore[import]

from .config import LOGGER
from .database import interface
from .k8s import KubeAPITools


def enqueue(job_obj: kubernetes.client.V1Job) -> None:
    """Enqueue k8s job to be started by job-starter thread."""
    timestamp = time.time()


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
            job_obj = entry.serialized_k8s_job_obj  # TODO
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
