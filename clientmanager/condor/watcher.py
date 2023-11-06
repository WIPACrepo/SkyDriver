"""For watching Skymap Scanner clients on an HTCondor cluster."""


import time
from pprint import pformat
from typing import Any

import htcondor  # type: ignore[import]

from ..config import LOGGER
from . import condor_tools


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

    def update_stored_job_attrs(ad: Any) -> None:
        if "ProcId" not in ad:
            return
        for attr in ad:
            if attr.startswith("HTChirp"):
                job_attrs[int(ad["ProcId"])][attr] = ad[attr]
        try:
            job_attrs[int(ad["ProcId"])]["status"] = condor_tools.job_status_to_str(
                int(ad["JobStatus"])
            )
        except Exception as e:
            LOGGER.exception(e)
            return

    def status_counts() -> dict[str, int]:
        cts = {}
        statuses = [a["status"] for a in job_attrs.values()]
        for status in set(statuses):
            cts[status] = len([s for s in statuses if s == status])
        return cts

    again = True
    while again:
        again = False

        # class ad
        LOGGER.info("getting query classads...")
        try:
            ads = schedd_obj.query(
                f"ClusterId == {cluster_id}",
                # ["list", "of", "desired", "attributes"],
            )
            for i, ad in enumerate(ads):
                again = True  # we got data, so keep going
                LOGGER.debug(f"class ad #{i}")
                LOGGER.debug(ad)
                update_stored_job_attrs(ad)
        except Exception as e:
            LOGGER.exception(e)

        LOGGER.info(f"job statuses ({n_workers=})")
        LOGGER.info(f"{pformat(job_attrs, indent=4)}")
        LOGGER.info(f"{pformat(status_counts(), indent=4)}")

        # histories
        LOGGER.info("getting histories...")
        try:
            histories = schedd_obj.history(
                f"ClusterId == {cluster_id}",
                [],  # ["list", "of", "desired", "attributes"],
            )
            for i, history in enumerate(histories):
                again = True  # we got data, so keep going
                LOGGER.debug(f"history #{i}")
                LOGGER.debug(history)
                update_stored_job_attrs(history)
        except Exception as e:
            LOGGER.exception(e)

        LOGGER.info(f"job statuses ({n_workers=})")
        LOGGER.info(f"{pformat(job_attrs, indent=4)}")
        LOGGER.info(f"{pformat(status_counts(), indent=4)}")

        # jobEpochHistory
        LOGGER.info("getting job epoch histories...")
        try:
            histories = schedd_obj.jobEpochHistory(
                f"ClusterId == {cluster_id}",
                [],  # ["list", "of", "desired", "attributes"],
            )
            for i, history in enumerate(histories):
                again = True  # we got data, so keep going
                LOGGER.debug(f"jobEpochHistory #{i}")
                LOGGER.debug(history)
                update_stored_job_attrs(history)
        except Exception as e:
            LOGGER.exception(e)

        LOGGER.info(f"job statuses ({n_workers=})")
        LOGGER.info(f"{pformat(job_attrs, indent=4)}")
        LOGGER.info(f"{pformat(status_counts(), indent=4)}")

        # wait
        time.sleep(60)
        LOGGER.info("requesting again...")
