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


async def launch_a_scan(
    rc: RestClient,
    event_file: Path,
    cluster: str,
    n_workers: int,
    reco_algo: str,
) -> str:
    """Request to SkyDriver to scan an event."""
    body = {
        "reco_algo": reco_algo,
        "event_i3live_json": event_file.open().read().strip(),
        "nsides": {1: 0},  # {8: 0, 64: 12, 512: 24},
        "real_or_simulated_event": "real",
        "predictive_scanning_threshold": 1,  # 0.3,
        "cluster": {cluster: n_workers},
        "docker_tag": "latest",
        "max_pixel_reco_time": 30 * 60,  # seconds
        "scanner_server_memory": "1G",
        "priority": 99,
        "scanner_server_env": {
            "SKYSCAN_MINI_TEST": True,
        },
    }
    resp = await rc.request("POST", "/scan", body)

    print(resp["scan_id"], flush=True)
    return resp["scan_id"]  # type: ignore[no-any-return]


async def monitor(rc: RestClient, scan_id: str, log_file: Path | None = None) -> dict:
    """Monitor the event scan until its done.

    Return the result.
    """
    out = open(log_file, "w") if log_file else sys.stdout
    result_resp = {}

    resp = await rc.request("GET", f"/scan/{scan_id}/manifest")
    print(json.dumps(resp, indent=4), file=out, flush=True)

    # loop w/ sleep
    done = False
    while True:
        # get result
        try:
            result_resp = await rc.request("GET", f"/scan/{scan_id}/result")
            print(
                pformat(result_resp), file=out, flush=True
            )  # pprint doesn't have flush
            done = result_resp["is_final"]
        except Exception as e:  # 404 (scanner not yet online)
            print(f"ok: {repr(e)}", file=out, flush=True)

        # get progress
        try:
            resp = await rc.request("GET", f"/scan/{scan_id}/manifest")
            progress = resp["progress"]
            print(
                json.dumps(progress["processing_stats"].pop("rate"), indent=4),
                file=out,
                flush=True,
            )
            print(json.dumps(progress, indent=4), file=out, flush=True)
        except Exception as e:
            # 404 (scanner not yet online) or KeyError (no progress yet)
            print(f"ok: {repr(e)}", file=out, flush=True)

        # done? else, wait
        print(scan_id, file=out, flush=True)
        if done:
            print("scan is done!", file=out, flush=True)
            return result_resp["skyscan_result"]
        await asyncio.sleep(60)
