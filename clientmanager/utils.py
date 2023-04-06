"""General Utilities."""


import os

import htcondor  # type: ignore[import]
from rest_tools.client import RestClient

from .config import ENV, LOGGER


def connect_to_skydriver() -> RestClient | None:
    """Connect to SkyDriver REST server, if the needed env vars are present.

    Also return the scan id.
    """
    if not (address := ENV.SKYSCAN_SKYDRIVER_ADDRESS):
        LOGGER.warning("Not connecting to SkyDriver")
        return None

    if not ENV.SKYSCAN_SKYDRIVER_SCAN_ID:
        raise RuntimeError(
            "Cannot connect to SkyDriver without `SKYSCAN_SKYDRIVER_SCAN_ID`"
        )

    skydriver_rc = RestClient(address, token=os.getenv("SKYSCAN_SKYDRIVER_AUTH"))
    LOGGER.info("Connected to SkyDriver")

    return skydriver_rc


def update_skydriver(
    skydriver_rc: RestClient,
    submit_result_obj: htcondor.SubmitResult,  # pylint:disable=no-member
    collector: str,
    schedd: str,
) -> None:
    """Send SkyDriver updates from the `submit_result`."""
    skydriver_rc.request_seq(
        "PATCH",
        f"/scan/manifest/{ENV.SKYSCAN_SKYDRIVER_SCAN_ID}",
        {
            "condor_cluster": {
                "collector": collector,
                "schedd": schedd,
                "cluster_id": submit_result_obj.cluster(),
                "jobs": submit_result_obj.num_procs(),
            }
        },
    )
