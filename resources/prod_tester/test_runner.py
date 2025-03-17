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
        "priority": -1,
        "scanner_server_env": {
            "SKYSCAN_MINI_TEST": True,
            "_SKYSCAN_CI_MINI_TEST": True,  # env var changed to this in the "skydriver 2"-ready scanner
        },
        "classifiers": {
            "_TEST": True,
        },
    }
    manifest = await rc.request("POST", "/scan", body)

    print(manifest["scan_id"], flush=True)
    return manifest  # type: ignore[no-any-return]


async def monitor(  # noqa: C901
    rc: RestClient,
    scan_id: str,
    log_file: Path | None = None,
) -> dict:
    """Monitor the event scan until its done.

    Return the result.
    """
    out = open(log_file, "w") if log_file else sys.stdout

    def print_now(_what: str | list | dict | None) -> None:
        if not isinstance(_what, str):
            _what = json.dumps(_what, indent=4)
        print(_what, file=out, flush=True)  # fyi: pprint doesn't have flush

    resp = await rc.request("GET", f"/scan/{scan_id}/manifest")
    print_now(resp)

    prev_status: dict = {}
    prev_progress: dict = {}
    prev_result: dict = {}

    # loop w/ sleep
    done = False
    while not done:
        print_now("-" * 60)

        # get status
        try:
            resp = await rc.request("GET", f"/scan/{scan_id}/status")
            if prev_status != resp:
                print_now(resp)
                prev_status = resp
            else:
                print_now("<no change in status>")
            done = resp["scan_complete"]  # loop control
        except Exception as e:  # 404 (scanner not yet online)
            print_now(f"suppressed error: {repr(e)}")

        # get manifest.progress
        try:
            resp = (await rc.request("GET", f"/scan/{scan_id}/manifest"))["progress"]
            if prev_progress != resp:
                print_now(resp)
                prev_progress = resp
            else:
                print_now("<no change in manifest.progress>")
        except Exception as e:
            # 404 (scanner not yet online) or KeyError (no progress yet)
            print_now(f"suppressed error: {repr(e)}")

        # get result
        try:
            resp = await rc.request("GET", f"/scan/{scan_id}/result")
            if prev_result != resp:
                print_now(resp)
                prev_result = resp
            else:
                print_now("<no change in result>")
        except Exception as e:
            print_now(f"suppressed error: {repr(e)}")

        # done? else, wait
        if not done:
            print_now(scan_id)
            await asyncio.sleep(60)

    print_now("scan is done!")
    print_now(scan_id)
    return (await rc.request("GET", f"/scan/{scan_id}/result"))["skyscan_result"]
