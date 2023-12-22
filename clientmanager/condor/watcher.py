"""For watching Skymap Scanner clients on an HTCondor cluster."""


import collections
import logging
import time
from pprint import pformat
from typing import Any, Iterator

import htcondor  # type: ignore[import-untyped]
from rest_tools.client import RestClient

from .. import utils
from ..config import WATCHER_INTERVAL, WATCHER_MAX_RUNTIME, WATCHER_N_TOP_TASK_ERRORS
from . import condor_tools as ct

LOGGER = logging.getLogger(__name__)


PROJECTION = [
    "ClusterId",
    "JobStatus",
    "EnteredCurrentStatus",
    "ProcId",
    #
    "HoldReason",
    "HoldReasonCode",
    "HoldReasonSubCode",
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


DONE_JOB_STATUSES: list[int] = [
    ct.REMOVED,
    ct.COMPLETED,
    ct.HELD,
]
NON_RESPONSE_LIMIT = 10


def _translate_special_attrs(job_ad: dict[str, Any]) -> None:
    """Special handling for specific attrs."""
    for attr in job_ad:
        if attr.startswith("HTChirp"):
            # unquote
            if isinstance(job_ad[attr], str):
                try:
                    job_ad[attr] = htcondor.classad.unquote(job_ad[attr])
                except Exception:
                    # LOGGER.error(f"could not unquote: {job[attr]}")
                    # LOGGER.exception(e)
                    pass
    try:
        job_ad["JobStatus"] = int(job_ad["JobStatus"])
    except Exception as e:
        LOGGER.exception(e)


def update_stored_job_infos(
    job_infos: dict[int, dict[str, Any]],
    classad: Any,
    source: str,
) -> None:
    """Update the job's classad attrs in `job_infos`."""
    procid = int(classad["ProcId"])
    job_infos[procid]["source"] = source
    job_infos[procid].update(dict(classad))  # start with everything
    _translate_special_attrs(job_infos[procid])


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
                # LOGGER.info(f"looking at job {classad['ProcId']}")
                # LOGGER.debug(str(call))
                # LOGGER.debug(classad)
                yield classad, call.__name__
        except Exception as e:
            LOGGER.exception(e)


def get_aggregate_statuses(
    job_infos: dict[int, dict[str, Any]],
    previous: dict[str, dict[str, int]],
) -> tuple[dict[str, dict[str, int]], bool]:
    """Aggregate statuses of jobs & return whether this is an new value."""

    def transform_job_status_val(info: dict[str, Any]) -> str:
        """Get job status -- transforming any as needed.

        NOTE: each transformation needs to be generic
        enough to aggregate nicely with others; e.g. don't
        append a timestamp, do append a standard reason str.
        """
        if info["JobStatus"] == ct.HELD:
            codes = (
                info.get("HoldReasonCode", None),
                info.get("HoldReasonSubCode", None),
            )
            return (
                f"{ct.job_status_to_str(ct.HELD)}: "
                f"{codes} "
                f"{info.get('HoldReason', 'unknown reason')}"
            )
        else:
            return ct.job_status_to_str(info["JobStatus"])

    statuses: dict[str, dict[str, int]] = {
        k: {}
        for k in set(transform_job_status_val(info) for info in job_infos.values())
    }

    for job_status in statuses:
        ids_for_this_job_status = [  # subset of job_infos ids
            i
            for i, info in job_infos.items()
            if transform_job_status_val(info) == job_status
        ]
        # NOTE - if the pilot did not send a status (ex: Held job), it is `None`
        statuses[job_status] = dict(
            collections.Counter(
                job_infos[i]["HTChirpEWMSPilotStatus"] for i in ids_for_this_job_status
            )
        )

    return statuses, statuses != previous


def get_aggregate_top_task_errors(
    job_infos: dict[int, dict[str, Any]],
    n_top_task_errors: int,
    previous: dict[str, int],
) -> tuple[dict[str, int], bool]:
    """Aggregate top X errors of jobs & return whether this is an new value."""
    counts = collections.Counter(
        dicto.get("HTChirpEWMSPilotError") for dicto in job_infos.values()
    )
    counts.pop(None, None)  # remove counts of "no error"

    errors = dict(counts.most_common(n_top_task_errors))
    return errors, errors != previous  # type: ignore[return-value]


def watch(
    collector: str,
    schedd: str,
    cluster_id: str,
    schedd_obj: htcondor.Schedd,
    n_workers: int,
    #
    skydriver_rc: RestClient,
    skydriver_cluster_obj: dict[str, Any],
) -> None:
    """Main logic."""
    LOGGER.info(
        f"Watching Skymap Scanner client workers on {cluster_id} / {collector} / {schedd}"
    )

    job_infos: dict[int, dict[str, Any]] = {
        i: {  # NOTE - it's important that attrs reported on later are `None` to start
            "JobStatus": None,
            "HTChirpEWMSPilotStatus": None,
        }
        for i in range(n_workers)
    }

    start = time.time()
    non_response_ct = 0
    aggregate_statuses: dict[str, dict[str, int]] = {}
    aggregate_top_task_errors: dict[str, int] = {}

    def keep_watching() -> bool:
        """
        NOTE - condor may be lagging, so we can't just quit when
        all jobs are done, since there may be more attrs to be updated.
        """
        if not any(  # if no done jobs, then keep going always
            job_infos[j]["JobStatus"] in DONE_JOB_STATUSES for j in job_infos
        ):
            return True
        else:
            # condor may occasionally slow down & prematurely return nothing
            return non_response_ct < NON_RESPONSE_LIMIT  # allow X non-responses

    # WATCHING LOOP
    while (
        keep_watching()
        and time.time() - start
        < WATCHER_MAX_RUNTIME  # just in case, stop if taking too long
    ):
        # wait -- sleeping at top guarantees this happens
        time.sleep(WATCHER_INTERVAL)
        LOGGER.info("(re)checking jobs...")

        # query
        classads = iter_job_classads(
            schedd_obj,
            (
                f"ClusterId == {cluster_id} && "
                # only care about "older" status jobs if they are RUNNING
                f"( JobStatus == {ct.RUNNING} || EnteredCurrentStatus >= {int(time.time()) - WATCHER_INTERVAL*3} )"
            ),
            PROJECTION,
        )
        non_response_ct += 1  # just in case
        for ad, source in classads:
            non_response_ct = 0
            update_stored_job_infos(job_infos, ad, source)
            # NOTE - if memory becomes an issue, switch to an in-iterator design

        # aggregate
        aggregate_statuses, has_new_statuses = get_aggregate_statuses(
            job_infos,
            aggregate_statuses,
        )
        aggregate_top_task_errors, has_new_errors = get_aggregate_top_task_errors(
            job_infos,
            WATCHER_N_TOP_TASK_ERRORS,
            aggregate_top_task_errors,
        )

        # log
        LOGGER.info(f"job aggregate statuses ({n_workers=})")
        LOGGER.info(f"{pformat(aggregate_statuses, indent=4)}")
        LOGGER.info(
            f"job aggregate top {WATCHER_N_TOP_TASK_ERRORS} task errors ({n_workers=})"
        )
        LOGGER.info(f"{pformat(aggregate_top_task_errors, indent=4)}")

        # figure updates
        if not has_new_statuses and not has_new_errors:
            LOGGER.info("no updates")
        else:
            # send updates
            LOGGER.info("sending updates to skydriver")
            utils.update_skydriver(
                skydriver_rc,
                **skydriver_cluster_obj,
                statuses=aggregate_statuses,
                top_task_errors=aggregate_top_task_errors,
            )
