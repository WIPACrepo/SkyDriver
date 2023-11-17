"""For watching Skymap Scanner clients on an HTCondor cluster."""


import time
from datetime import datetime as dt
from pprint import pformat
from typing import Any, Iterator

import htcondor  # type: ignore[import]

from ..config import LOGGER, WATCHER_INTERVAL
from . import condor_tools as ct

PROJECTION = [
    "ClusterId",
    "JobStatus",
    "EnteredCurrentStatus",
    "ProcId",
    #
    "HTChirpEWMSPilotLastUpdatedTimestamp",
    "HTChirpEWMSPilotStartedTimestamp",
    "HTChirpEWMSPilotStatus",
    #
    "HTChirpEWMSPilotTasksTotal",
    "HTChirpEWMSPilotTasksFailed",
    "HTChirpEWMSPilotTasksSuccess",
    #
    "HTChirpEWMSPilotError",
    "HTChirpEWMSPilotErrorTraceback",
]


DONE_JOB_STATUSES = [
    ct.job_status_to_str(ct.REMOVED),
    ct.job_status_to_str(ct.COMPLETED),
]
NON_RESPONSE_LIMIT = 10


def update_stored_job_attrs(
    job_attrs: dict[int, dict[str, Any]],
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
            if attr.endswith("Timestamp"):
                job_attrs[procid][attr] = str(dt.fromtimestamp(float(val)))
                # TODO use float if sending to skydriver
            else:
                job_attrs[procid][attr] = val
    try:
        job_attrs[procid]["JobStatus"] = ct.job_status_to_str(int(classad["JobStatus"]))
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


def aggregate_statuses(
    job_attrs: dict[int, dict[str, Any]]
) -> dict[str, dict[str, int]]:
    """Aggregate statuses of jobs."""

    def counts(key: str) -> dict[str, int]:
        all_statuses = [a[key] for a in job_attrs.values()]
        return {
            status: len([s for s in all_statuses if s == status])
            for status in set(all_statuses)
        }

    return {
        k: counts(k)
        for k in [
            "JobStatus",
            "HTChirpEWMSPilotStatus",
        ]
    }


def watch(
    collector: str,
    schedd: str,
    cluster_id: str,
    schedd_obj: htcondor.Schedd,
    n_workers: int,
    cluster_uuid: str,
) -> None:
    """Main logic."""
    LOGGER.info(
        f"Watching Skymap Scanner client workers on {cluster_id} / {collector} / {schedd}"
    )

    job_attrs: dict[int, dict[str, Any]] = {
        i: {
            "JobStatus": None,
            "HTChirpEWMSPilotStatus": None,
        }
        for i in range(n_workers)
    }

    start = time.time()
    non_response_ct = 0

    def keep_watching() -> bool:
        """
        NOTE - condor may be lagging, so we can't just quit when
        all jobs are done, since there may be more attrs to be updated.
        """
        if not any(  # but only if we have done jobs
            job_attrs[j]["JobStatus"] in DONE_JOB_STATUSES for j in job_attrs
        ):
            return True
        # condor may occasionally slow down & prematurely return nothing
        return non_response_ct < NON_RESPONSE_LIMIT  # allow X non-responses

    # WATCHING LOOP
    while (
        keep_watching()
        and time.time() - start < 60 * 60 * 24  # just in case, stop if taking too long
    ):
        classads = iter_job_classads(
            schedd_obj,
            (
                f"ClusterId == {cluster_id} && "
                # only care about "older" status jobs if they are RUNNING
                f"( JobStatus == {ct.RUNNING} || EnteredCurrentStatus >= {int(time.time()) - WATCHER_INTERVAL*5} )"
            ),
            PROJECTION,
        )
        non_response_ct += 1  # just in case
        for ad, source in classads:
            non_response_ct = 0
            update_stored_job_attrs(job_attrs, ad, source)

        LOGGER.info(f"job statuses ({n_workers=})")
        LOGGER.info(f"{pformat(job_attrs, indent=4)}")
        LOGGER.info(f"{pformat(aggregate_statuses(job_attrs), indent=4)}")

        # wait
        time.sleep(WATCHER_INTERVAL)
        LOGGER.info("checking jobs again...")
