"""An example script for scanning multiple events.

This script is meant to be a starting point--many arguments and options
will need to be tweaked and/or parameterized.
"""

import argparse
from pathlib import Path

from rest_tools.client import RestClient, SavedDeviceGrantAuth


def get_rest_client() -> RestClient:
    """Get REST client for talking to SkyDriver."""
    token = SavedDeviceGrantAuth(
        "",
        token_url="https://keycloak.icecube.wisc.edu/auth/realms/IceCube",
        filename="device-refresh-token",
        client_id="skydriver-external",
    )._openid_token()

    # NOTE: if you want to automate this script, write 'token' to a file -- it will expire <1 day

    rc = RestClient(
        "https://skydriver.icecube.aq",
        token=token,
        retries=0,
    )
    return rc


def launch_a_scan(rc: RestClient, event_file: Path) -> str:
    """Request to SkyDriver to scan an event."""
    body = {
        "reco_algo": "millipede_wilks",
        "event_i3live_json": event_file.open().read().strip(),
        "nsides": {8: 12, 64: 12, 512: 24},
        "real_or_simulated_event": "simulated",
        "predictive_scanning_threshold": 0.3,
        "cluster": {"sub-2": 1500},
        "docker_tag": "latest",
    }
    resp = rc.request_seq("POST", "/scan", body)

    # print(resp["scan_id"])
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
    args = parser.parse_args()

    rc = get_rest_client()
    for event_file in args.event_files.iterdir():
        print("-----------------------")
        print(event_file)
        scan_id = launch_a_scan(rc, args.event_file)
        print(f"SCAN ID: {scan_id}")


if __name__ == "__main__":
    main()
