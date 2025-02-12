"""An simple script for checking if all the components are working.

Scans an event & monitors its progress.

**NOTE:**
This script is not meant to be used for any physics! It may have
unrealistic settings that have adverse effects. It also may be broken,
or changed fundamentally without notice. You have been warned!
"""

import asyncio
import json
import logging
import sys
from pathlib import Path
from pprint import pformat

from rest_tools.client import RestClient, SavedDeviceGrantAuth


def get_rest_client(skydriver_url: str) -> RestClient:
    """Get REST client for talking to SkyDriver.

    This will present a QR code in the terminal for initial validation.
    """
    if "://" not in skydriver_url:
        skydriver_url = "https://" + skydriver_url
    logging.info(f"connecting to {skydriver_url}...")

    # NOTE: If your script will not be interactive (like a cron job),
    # then you need to first run your script manually to validate using
    # the QR code in the terminal.

    return SavedDeviceGrantAuth(
        skydriver_url,
        token_url="https://keycloak.icecube.wisc.edu/auth/realms/IceCube",
        filename="device-refresh-token",
        client_id="skydriver-external",
        retries=10,
    )


async def rescan_a_scan(rc: RestClient, rescan_origin_id: str) -> dict:
    """Request to SkyDriver to rescan."""
    manifest = await rc.request("POST", f"/scan/{rescan_origin_id}/actions/rescan")

    print(manifest["scan_id"], flush=True)
    return manifest  # type: ignore[no-any-return]


async def launch_a_scan(
    rc: RestClient,
    event_file: Path,
    cluster: str,
    n_workers: int,
    reco_algo: str,
    skyscan_docker_tag: str,
) -> dict:
    """Request to SkyDriver to scan an event."""
    body = {
        "reco_algo": reco_algo,
        "event_i3live_json": event_file.open().read().strip(),
        "nsides": {1: 0},  # {8: 0, 64: 12, 512: 24},
        "real_or_simulated_event": "real",
        "predictive_scanning_threshold": 1,  # 0.3,
        "cluster": {cluster: n_workers},
        "docker_tag": skyscan_docker_tag,
        "max_pixel_reco_time": 30 * 60,  # seconds
        "scanner_server_memory": "1G",
        "priority": 100,
        "scanner_server_env": {
            "SKYSCAN_MINI_TEST": True,
        },
        "classifiers": {
            "_TEST": True,
        },
    }
    manifest = await rc.request("POST", "/scan", body)

    print(manifest["scan_id"], flush=True)
    return manifest  # type: ignore[no-any-return]


async def monitor(rc: RestClient, scan_id: str, log_file: Path | None = None) -> dict:
    """Monitor the event scan until its done.

    Return the result.
    """
    out = open(log_file, "w") if log_file else sys.stdout
    resp = await rc.request("GET", f"/scan/{scan_id}/manifest")
    print(json.dumps(resp, indent=4), file=out, flush=True)

    # loop w/ sleep
    while True:
        print("-" * 60, file=out, flush=True)
        # get result
        try:
            resp = await rc.request("GET", f"/scan/{scan_id}/status")
            print(pformat(resp), file=out, flush=True)  # pprint doesn't have flush
        except Exception as e:  # 404 (scanner not yet online)
            print(f"suppressed error: {repr(e)}", file=out, flush=True)

        # get progress
        try:
            resp = await rc.request("GET", f"/scan/{scan_id}/manifest")
            print(json.dumps(resp["progress"], indent=4), file=out, flush=True)
        except Exception as e:
            # 404 (scanner not yet online) or KeyError (no progress yet)
            print(f"suppressed error: {repr(e)}", file=out, flush=True)

        # get status
        try:
            resp = await rc.request("GET", f"/scan/{scan_id}/result")
            print(pformat(resp), file=out, flush=True)  # pprint doesn't have flush
        except Exception as e:
            print(f"suppressed error: {repr(e)}", file=out, flush=True)
        else:
            if resp["scan_complete"]:
                print("scan is done!", file=out, flush=True)
                print(scan_id, file=out, flush=True)
                return resp["skyscan_result"]

        # done? else, wait
        print(scan_id, file=out, flush=True)
        await asyncio.sleep(60)
