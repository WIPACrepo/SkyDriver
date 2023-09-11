"""An example script for scanning multiple events.

This script is meant to be a starting point--many arguments and options
will need to be tweaked and/or parameterized.
"""

import argparse
import random

# import time
from pathlib import Path

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


def launch_a_scan(
    rc: RestClient, event_file: Path, cluster: str, n_workers: int
) -> str:
    """Request to SkyDriver to scan an event."""
    body = {
        "reco_algo": "millipede_wilks",
        "event_i3live_json": event_file.open().read().strip(),
        "nsides": {8: 0, 64: 12, 512: 24},
        "real_or_simulated_event": "simulated",
        "predictive_scanning_threshold": 0.3,
        "cluster": {cluster: n_workers},
        "docker_tag": "latest",
        "max_pixel_reco_time": 60 * 60 * 1,
    }
    resp = rc.request_seq("POST", "/scan", body)

    print(resp["scan_id"])
    return resp["scan_id"]  # type: ignore[no-any-return]


def main() -> None:
    """Launch scans for multiple events."""

    parser = argparse.ArgumentParser(
        description="Launch scans for multiple events",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--event-files",
        required=True,
        type=Path,
        help="a directory of event files in realtime's JSON format",
    )
    parser.add_argument(
        "--n-events",
        default=None,
        type=int,
        help="the number of events to scan, in case you don't want to scan all the files",
    )
    parser.add_argument(
        "--cluster",
        required=True,
        help="the cluster to use for running workers. Ex: sub-2",
    )
    parser.add_argument(
        "--n-workers",
        required=True,
        type=int,
        help="number of workers to request",
    )
    args = parser.parse_args()

    rc = get_rest_client()
    files = list(args.event_files.iterdir())
    random.shuffle(files)
    for i, event_file in enumerate(files):
        if event_file.suffix != '.json':
            continue
        print("-----------------------")
        print(event_file)
        scan_id = launch_a_scan(rc, event_file, args.cluster, args.n_workers)
        print(f"SCAN ID: {scan_id}")
        if args.n_events and i == args.n_events - 1:
            break
        # time.sleep(60 * 12)


if __name__ == "__main__":
    main()
