"""For stopping Skymap Scanner clients on an HTCondor cluster."""


import logging

import htcondor  # type: ignore[import-untyped]

LOGGER = logging.getLogger(__name__)


def stop(
    collector: str,
    schedd: str,
    cluster_id: str,
    schedd_obj: htcondor.Schedd,
) -> None:
    """Main logic."""
    LOGGER.info(
        f"Stopping Skymap Scanner client workers on {cluster_id} / {collector} / {schedd}"
    )

    # Remove workers -- may not be instantaneous
    LOGGER.info("Requesting removal...")
    act_obj = schedd_obj.act(
        htcondor.JobAction.Remove,
        f"ClusterId == {cluster_id}",
        reason="Requested by SkyDriver",
    )
    LOGGER.debug(act_obj)
    LOGGER.info(f"Removed {act_obj['TotalSuccess']} workers")

    # TODO: get/forward worker logs
