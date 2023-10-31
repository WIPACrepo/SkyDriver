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

    statuses: dict[int, str] = {i: "Unknown" for i in range(n_workers)}

    def update_job_status(ad: Any) -> None:
        try:
            string = condor_tools.job_status_to_str(int(ad["JobStatus"]))
        except Exception as e:
            LOGGER.exception(e)
            return
        try:
            statuses[int(ad["ProcId"])] = string
        except Exception as e:
            LOGGER.exception(e)
            return

    def counts() -> dict[str, int]:
        cts = {}
        for status in set(statuses.values()):
            cts[status] = len([s for s in statuses.values() if s == status])
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
        except Exception as e:
            LOGGER.exception(e)
        else:
            for i, ad in enumerate(ads):
                again = True  # we got data, so keep going
                LOGGER.debug(f"class ad #{i}")
                LOGGER.debug(ad)
                update_job_status(ad)

        LOGGER.info(f"job statuses ({n_workers=})")
        LOGGER.info(f"{pformat(statuses, indent=4)}")
        LOGGER.info(f"{pformat(counts(), indent=4)}")

        # histories
        LOGGER.info("getting histories...")
        try:
            histories = schedd_obj.history(
                f"ClusterId == {cluster_id}",
                [],  # ["list", "of", "desired", "attributes"],
            )
        except Exception as e:
            LOGGER.exception(e)
        else:
            for i, history in enumerate(histories):
                again = True  # we got data, so keep going
                LOGGER.debug(f"history #{i}")
                LOGGER.debug(history)
                update_job_status(history)

        LOGGER.info(f"job statuses ({n_workers=})")
        LOGGER.info(f"{pformat(statuses, indent=4)}")
        LOGGER.info(f"{pformat(counts(), indent=4)}")

        # jobEpochHistory
        LOGGER.info("getting job epoch histories...")
        try:
            histories = schedd_obj.jobEpochHistory(
                f"ClusterId == {cluster_id}",
                [],  # ["list", "of", "desired", "attributes"],
            )
        except Exception as e:
            LOGGER.exception(e)
        else:
            for i, history in enumerate(histories):
                again = True  # we got data, so keep going
                LOGGER.debug(f"jobEpochHistory #{i}")
                LOGGER.debug(history)
                update_job_status(history)

        LOGGER.info(f"job statuses ({n_workers=})")
        LOGGER.info(f"{pformat(statuses, indent=4)}")
        LOGGER.info(f"{pformat(counts(), indent=4)}")

        # wait
        time.sleep(60)
        LOGGER.info("requesting again...")
