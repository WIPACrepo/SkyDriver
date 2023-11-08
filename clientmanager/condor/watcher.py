"""For watching Skymap Scanner clients on an HTCondor cluster."""


import time
from datetime import datetime as dt
from pprint import pformat
from typing import Any, Iterator

import htcondor  # type: ignore[import]

from ..config import LOGGER
from .condor_tools import job_status_to_str


def update_stored_job_attrs(
    job_attrs: dict[int, dict[str, str]],
    classad: Any,
    source: str,
) -> None:
    """Update the job's classad attrs in `job_attrs`."""
    procid = int(classad["ProcId"])
    job_attrs[procid]["source"] = source
    for attr in classad:
        if attr.startswith("HTChirp"):
            if isinstance(classad[attr], str):
                try:
                    val = htcondor.classad.unquote(classad[attr])
                except Exception as e:
                    LOGGER.error(f"could not unquote: {classad[attr]}")
                    LOGGER.exception(e)
                    val = classad[attr]
            else:
                val = classad[attr]
            if attr.endswith("_Timestamp"):
                job_attrs[procid][attr] = str(dt.fromtimestamp(float(val)))
                # TODO use float if sending to skydriver
            else:
                job_attrs[procid][attr] = val
    try:
        job_attrs[procid]["status"] = job_status_to_str(int(classad["JobStatus"]))
    except Exception as e:
        LOGGER.exception(e)


def iter_job_classads(
    schedd_obj: htcondor.Schedd,
    constraint: str,
    projection: list[str],
) -> Iterator[tuple[htcondor.classad.ClassAd, str]]:
    """Get the job class ads, trying various sources.

    May not get all of them.
    """
    for call in [
        schedd_obj.query,
        schedd_obj.history,
        schedd_obj.jobEpochHistory,
    ]:
        try:
            for classad in call(constraint, projection):
                if "ProcId" not in classad:
                    continue
                LOGGER.info(f"looking at job {classad['ProcId']}")
                LOGGER.debug(str(call))
                LOGGER.debug(classad)
                yield classad, call.__name__
        except Exception as e:
            LOGGER.exception(e)


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

    projection: list[str] = []  # TODO
    start = time.time()

    while (
        not all(job_attrs[j]["status"] == job_status_to_str(4) for j in job_attrs)
        and time.time() - start < 60 * 60 * 24  # TODO - be smarter
    ):
        classads = iter_job_classads(
            schedd_obj,
            f"ClusterId == {cluster_id}",
            projection,
        )
        for ad, source in classads:
            if job_attrs[int(ad["ProcId"])]["status"] == job_status_to_str(4):
                continue  # no need to update completed jobs
            update_stored_job_attrs(job_attrs, ad, source)

        LOGGER.info(f"job statuses ({n_workers=})")
        LOGGER.info(f"{pformat(job_attrs, indent=4)}")
        LOGGER.info(f"{pformat(status_counts(job_attrs), indent=4)}")

        # wait
        time.sleep(60)
        LOGGER.info("checking jobs again...")
