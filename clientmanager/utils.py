"""General Utilities."""


import os

from rest_tools.client import RestClient

from .config import LOGGER


def connect_to_skydriver() -> tuple[RestClient | None, str]:
    """Connect to SkyDriver REST server, if the needed env vars are present.

    Also return the scan id.
    """
    address = os.getenv("SKYSCAN_SKYDRIVER_ADDRESS")
    if not address:
        LOGGER.warning("Not connecting to SkyDriver")
        return None, ""

    scan_id = os.getenv("SKYSCAN_SKYDRIVER_SCAN_ID")
    if not scan_id:
        raise RuntimeError(
            "Cannot connect to SkyDriver without `SKYSCAN_SKYDRIVER_SCAN_ID`"
        )

    skydriver_rc = RestClient(address, token=os.getenv("SKYSCAN_SKYDRIVER_AUTH"))
    LOGGER.info("Connected to SkyDriver")

    return skydriver_rc, scan_id
