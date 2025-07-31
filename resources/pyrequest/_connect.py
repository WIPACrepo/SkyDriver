"""Make remote connections."""

import logging
from pathlib import Path

from rest_tools.client import RestClient, SavedDeviceGrantAuth


def get_rest_client(skydriver_type: str) -> RestClient:
    """Get REST client for talking to SkyDriver.

    This will present a QR code in the terminal for initial validation.
    """
    match skydriver_type:
        case "prod":
            name = "skydriver"
        case "dev":
            name = "skydriver-dev"
        case _:
            raise ValueError(f"Unknown skydriver type {skydriver_type}")

    skydriver_url = f"https://{name}.icecube.aq"
    logging.info(f"connecting to {skydriver_url}...")

    # NOTE: If your script will not be interactive (like a cron job),
    # then you need to first run your script manually to validate using
    # the QR code in the terminal.

    return SavedDeviceGrantAuth(
        skydriver_url,
        token_url="https://keycloak.icecube.wisc.edu/auth/realms/IceCube",
        filename=str(Path(f"~/device-refresh-token-{name}").expanduser().resolve()),
        client_id="skydriver-external",
        retries=0,
    )
