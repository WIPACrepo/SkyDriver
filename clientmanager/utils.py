"""General Utilities."""


import os

from rest_tools.client import RestClient

from .config import ENV, LOGGER


def connect_to_skydriver() -> tuple[RestClient | None, str]:
    """Connect to SkyDriver REST server, if the needed env vars are present.

    Also return the scan id.
    """
    if not (address := ENV.SKYSCAN_SKYDRIVER_ADDRESS):
        LOGGER.warning("Not connecting to SkyDriver")
        return None, ""

    scan_id = os.getenv("")
    if not (scan_id := ENV.SKYSCAN_SKYDRIVER_SCAN_ID):
        raise RuntimeError(
            "Cannot connect to SkyDriver without `SKYSCAN_SKYDRIVER_SCAN_ID`"
        )

    skydriver_rc = RestClient(address, token=os.getenv("SKYSCAN_SKYDRIVER_AUTH"))
    LOGGER.info("Connected to SkyDriver")

    return skydriver_rc, scan_id
