"""For watching Skymap Scanner clients on an HTCondor cluster."""


import time

import htcondor  # type: ignore[import]

from ..config import LOGGER


def watch(
    collector: str,
    schedd: str,
    cluster_id: str,
    schedd_obj: htcondor.Schedd,
) -> None:
    """Main logic."""
    LOGGER.info(
        f"Watching Skymap Scanner client workers on {cluster_id} / {collector} / {schedd}"
    )
    start = time.time()

    while time.time() - start < 24 * 60 * 60:  # only go for 1 day -- TODO smarten
        # class ad
        LOGGER.info("getting query classads...")
        try:
            ads = schedd_obj.query(
                f"ClusterId == {cluster_id}",
                # ["list", "of", "desired", "attributes"],
            )
            for i, ad in enumerate(ads):
                LOGGER.debug(f"class ad #{i}")
                LOGGER.debug(ad)
        except Exception as e:
            LOGGER.debug(f"error -> {e}")

        # histories
        LOGGER.info("getting histories...")
        try:
            histories = schedd_obj.history(
                f"ClusterId == {cluster_id}",
                [],  # ["list", "of", "desired", "attributes"],
            )
            for i, history in enumerate(histories):
                LOGGER.debug(f"history #{i}")
                LOGGER.debug(history)
        except Exception as e:
            LOGGER.debug(f"error -> {e}")

        # jobEpochHistory
        LOGGER.info("getting job epoch histories...")
        try:
            histories = schedd_obj.jobEpochHistory(
                f"ClusterId == {cluster_id}",
                [],  # ["list", "of", "desired", "attributes"],
            )
            for i, history in enumerate(histories):
                LOGGER.debug(f"jobEpochHistory #{i}")
                LOGGER.debug(history)
        except Exception as e:
            LOGGER.debug(f"error -> {e}")

        # wait
        time.sleep(60)
        LOGGER.info("requesting again...")
