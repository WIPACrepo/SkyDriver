"""An example script for scanning an event & monitoring its progress.

This script is meant to be a starting point--many arguments and options
will need to be tweaked and/or parameterized.
"""

import argparse
import json
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
    rc: RestClient,
    event_file: Path,
    cluster: str,
    n_workers: int,
    max_pixel_reco_time: int,
    reco_algo: str,
    scanner_server_memory: str,
) -> str:
    """Request to SkyDriver to scan an event."""
    body = {
        "reco_algo": reco_algo,
        "event_i3live_json": event_file.open().read().strip(),
        "nsides": {8: 0, 64: 12, 512: 24},
        "real_or_simulated_event": "simulated",
        "predictive_scanning_threshold": 0.3,
        "cluster": {cluster: n_workers},
        "docker_tag": "latest",
        "max_pixel_reco_time": max_pixel_reco_time,
        "scanner_server_memory": scanner_server_memory,
    }
    resp = rc.request_seq("POST", "/scan", body)

    print(resp["scan_id"])
    return resp["scan_id"]  # type: ignore[no-any-return]


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
    parser.add_argument(
        "--max-pixel-reco-time",
        required=True,
        type=int,
        help="how long a reco should take",
    )
    parser.add_argument(
        "--reco-algo",
        required=False,
        default="millipede_wilks",
        help="reconstruction algorithm to run",
    )
    parser.add_argument(
        "--scanner-server-memory",
        required=False,
        default="512M",
        help="server memory required",
    )
    args = parser.parse_args()

    rc = get_rest_client()
    scan_id = launch_a_scan(
        rc,
        args.event_file,
        args.cluster,
        args.n_workers,
        args.max_pixel_reco_time,
        args.reco_algo,
        args.scanner_server_memory,
    )
    monitor(rc, scan_id)


if __name__ == "__main__":
    main()
