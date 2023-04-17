"""An example script for aborting a scan.

This script is meant to be a starting point--many arguments and options
will need to be tweaked and/or parameterized.
"""

import argparse

from rest_tools.client import RestClient, SavedDeviceGrantAuth


def get_rest_client() -> RestClient:
    """Get REST client for talking to SkyDriver.

    This will present a QR code in the terminal for initial validation.
    """

    # NOTE: If your script will not be interactive (like a cron job),
    # then you need to first run your script manually to validate using
    # the QR code in the terminal.

    return SavedDeviceGrantAuth(
        "https://skydriver.icecube.aq",
        token_url="https://keycloak.icecube.wisc.edu/auth/realms/IceCube",
        filename="device-refresh-token",
        client_id="skydriver-external",
        retries=0,
    )


def main() -> None:
    """Abort a scan of an event."""

    parser = argparse.ArgumentParser(
        description="Abort a scan for an event",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--scan-id",
        required=True,
        type=str,
        help="the id of the scan",
    )
    args = parser.parse_args()

    rc = get_rest_client()
    rc.request_seq("DELETE", f"/scan/{args.scan_id}")


if __name__ == "__main__":
    main()
