"""An example script for monitoring a scan.

This script is meant to be a starting point--many arguments and options
will need to be tweaked and/or parameterized.
"""

import argparse
import json
import time
from pprint import pprint

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


def monitor(rc: RestClient, scan_id: str) -> None:
    """Monitor the event scan until its done."""
    done = False
    while True:
        # get result
        try:
            result = rc.request_seq("GET", f"/scan/{scan_id}/result")
            pprint(result)
            done = result["is_final"]
        except Exception as e:  # 404 (scanner not yet online)
            print(e)

        # get progress
        try:
            progress = rc.request_seq("GET", f"/scan/{scan_id}/manifest")["progress"]
            print(json.dumps(progress["processing_stats"].pop("rate"), indent=4))
            print(json.dumps(progress, indent=4))
        except (
            Exception
        ) as e:  # 404 (scanner not yet online) or KeyError (no progress yet)
            print(e)

        # wait
        print(scan_id)
        if done:
            return
        time.sleep(60)


def main() -> None:
    """Watch a scan of an event."""

    parser = argparse.ArgumentParser(
        description="Watch a scan for an event",
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
    monitor(rc, args.scan_id)


if __name__ == "__main__":
    main()
