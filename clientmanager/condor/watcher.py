"""For watching Skymap Scanner clients on an HTCondor cluster."""


import time
import urllib
from datetime import datetime as dt
from pprint import pformat
from typing import Any

import htcondor  # type: ignore[import]

from ..config import LOGGER
from . import condor_tools


class ClassAdNotFound(Exception):
    """Raised when a class ad is not found."""


def update_stored_job_attrs(
    job_attrs: dict[int, dict[str, str]],
    classad: Any,
) -> None:
    """Update the job's classad attrs in `job_attrs`."""
    procid = int(classad["ProcId"])
    for attr in classad:
        if attr.startswith("HTChirp"):
            if isinstance(classad[attr], str):
                val = urllib.parse.unquote(classad[attr])
            else:
                val = classad[attr]
            if attr.endswith("_Timestamp"):
                job_attrs[procid][attr] = str(dt.fromtimestamp(float(val)))
                # TODO use float if sending to skydriver
            else:
                job_attrs[procid][attr] = val
    try:
        job_attrs[procid]["status"] = condor_tools.job_status_to_str(
            int(classad["JobStatus"])
        )
    except Exception as e:
        LOGGER.exception(e)
        return


def get_job_classad(
    schedd_obj: htcondor.Schedd,
    constraint: str,
    projection: list[str],
) -> htcondor.classad.ClassAd:
    """Get the job class ad, trying various sources."""
    for call in [
        schedd_obj.query,
        schedd_obj.history,
        schedd_obj.jobEpochHistory,
    ]:
        try:
            for classad in call(constraint, projection):
                LOGGER.debug(str(call))
                LOGGER.debug(classad)
                return classad
        except Exception as e:
            LOGGER.exception(e)
    raise ClassAdNotFound("could not find matching classad")


def status_counts(job_attrs: dict[int, dict[str, str]]) -> dict[str, int]:
    """Aggregate statuses of jobs."""
    cts = {}
    statuses = [a["status"] for a in job_attrs.values()]
    for status in set(statuses):
        cts[status] = len([s for s in statuses if s == status])
    return cts


def watch(
    collector: str,
    schedd: str,
    cluster_id: str,
    schedd_obj: htcondor.Schedd,
    n_workers: int,
) -> None:
    """Main logic."""
    LOGGER.info(
        f"Watching Skymap Scanner client workers on {cluster_id} / {collector} / {schedd}"
    )

    job_attrs: dict[int, dict[str, str]] = {
        i: {"status": "Unknown"} for i in range(n_workers)
    }

    # TODO - be smarter about queries, subset attrs & keep track of finished jobs
    #        (note: can go running -> idle -> running)

    projection = []
    start = time.time()

    while (
        not all(
            job_attrs[j]["status"] == condor_tools.job_status_to_str(4)
            for j in job_attrs
        )
        and time.time() - start < 60 * 60 * 24  # TODO - be smarter
    ):
        for job_id in job_attrs:
            if job_attrs[job_id]["status"] == condor_tools.job_status_to_str(4):
                continue
            LOGGER.info(f"looking at job {job_id}")

            try:
                classad = get_job_classad(
                    schedd_obj,
                    f"ClusterId == {cluster_id} && ProcId == {job_id}",
                    projection,
                )
            except ClassAdNotFound:
                continue
            update_stored_job_attrs(job_attrs, classad)

        LOGGER.info(f"job statuses ({n_workers=})")
        LOGGER.info(f"{pformat(job_attrs, indent=4)}")
        LOGGER.info(f"{pformat(status_counts(job_attrs), indent=4)}")

        # wait
        time.sleep(60)
        LOGGER.info("checking jobs again...")
