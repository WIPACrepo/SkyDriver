"""An example script for scanning an event & monitoring its progress.

This script is meant to be a starting point--many arguments and options
will need to be tweaked and/or parameterized.
"""

import argparse
import time
from pathlib import Path
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


def launch_a_scan(
    rc: RestClient, event_file: Path, cluster: str, n_workers: int
) -> str:
    """Request to SkyDriver to scan an event."""
    body = {
        "reco_algo": "millipede_wilks",
        "event_i3live_json": event_file.open().read().strip(),
        "nsides": {1: 12},
        "real_or_simulated_event": "simulated",
        "predictive_scanning_threshold": 0.3,
        "cluster": {cluster: n_workers},
        "docker_tag": "latest",
    }
    resp = rc.request_seq("POST", "/scan", body)

    print(resp["scan_id"])
    return resp["scan_id"]  # type: ignore[no-any-return]


def monitor(rc: RestClient, scan_id: str) -> None:
    """Monitor the event scan until its done."""
    while True:
        # get progress
        try:
            progress = rc.request_seq("GET", f"/scan/{scan_id}/manifest")["progress"]
            pprint(progress)
        except (
            Exception
        ) as e:  # 404 (scanner not yet online) or KeyError (no progress yet)
            print(e)

        # get result
        try:
            result = rc.request_seq("GET", f"/scan/{scan_id}/result")
            pprint(result)
            pprint(progress)
            if result["is_final"]:
                return
        except Exception as e:  # 404 (scanner not yet online)
            print(e)

        # wait
        time.sleep(60)


def main() -> None:
    """Launch and monitor a scan for an event."""

    parser = argparse.ArgumentParser(
        description="Launch and monitor a scan for an event",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--event-file",
        required=True,
        type=Path,
        help="the event in realtime's JSON format",
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
    scan_id = launch_a_scan(rc, args.event_file, args.cluster, args.n_workers)
    monitor(rc, scan_id)


if __name__ == "__main__":
    main()
