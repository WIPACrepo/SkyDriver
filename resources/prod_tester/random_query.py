"""A script to randomly query SkyDriver.

This will makes sure everything that should be accessible is accessible.
"""

import argparse
import asyncio
import logging
import pprint
import random
from collections import defaultdict
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


# Function to split list into chunks
def chunk_list(data, size):
    for i in range(0, len(data), size):
        yield data[i : i + size]


async def main():
    parser = argparse.ArgumentParser(
        description="Launch and monitor a scan for an event",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--skydriver",
        dest="skydriver_type",
        required=True,
        choices=["dev", "prod"],
        help=(
            "the type of the SkyDriver instance for REST API URL "
            "(ex: prod -> https://skydriver.icecube.aq; dev -> https://skydriver-dev.icecube.aq)"
        ),
    )
    args = parser.parse_args()

    rc = get_rest_client(args.skydriver_type)

    # 1: get all the scan_ids (not too large)
    print("POST @ /scans/find ...")
    resp = await rc.request(
        "POST",
        "/scans/find",
        {
            "filter": {},
            "include_deleted": True,
            "manifest_projection": ["scan_id"],
        },
    )
    scan_ids = [m["scan_id"] for m in resp["manifests"]]
    print(f"{len(scan_ids)} scans")
    scan_ids = random.sample(scan_ids, max(1, len(scan_ids) // 20))  # 5% sample
    print(f"will run queries for {len(scan_ids)} scans (5% sample)")

    print("\n---\n")

    # 2: re-find
    total = 0
    versions = defaultdict(list)
    for chunk_scan_ids in chunk_list(scan_ids, 10):
        print("POST @ /scans/find ...")
        resp = await rc.request(
            "POST",
            "/scans/find",
            {
                "filter": {"scan_id": {"$in": chunk_scan_ids}},
                "include_deleted": True,
            },
        )
        pprint.pprint(resp)
        print(f"found {len(resp['manifests'])}/{len(chunk_scan_ids)} scans (subset)")
        total += len(resp["manifests"])
        for m in resp["manifests"]:
            if m.get("i3_event_id"):
                versions["v1.2"].append(m["scan_id"])
            elif "i3_event_id" not in m:
                versions["<=v1.1"].append(m["scan_id"])
            else:
                versions["other"].append(m["scan_id"])
    pprint.pprint(versions)
    print(f"confirmed {total} scans")
    assert total == len(scan_ids)
    assert all(v for v in versions.values())  # check that all versions are represented

    print("\n---\n")

    # 3. quickly query the backlog
    print("GET @ /scans/backlog ...")
    resp = await rc.request("GET", "/scans/backlog")
    pprint.pprint(resp)

    print("\n---\n")

    # 4. query each scan
    for i, scan_id in enumerate(scan_ids):
        print(f"various queries for {scan_id} ({i + 1}/{len(scan_ids)}) ...")
        #
        print(f"GET @ /scan/{scan_id} ...")
        resp = await rc.request("GET", f"/scan/{scan_id}", {"include_deleted": True})
        pprint.pprint(resp)
        #
        print(f"GET @ /scan/{scan_id}/manifest ...")
        resp = await rc.request(
            "GET", f"/scan/{scan_id}/manifest", {"include_deleted": True}
        )
        pprint.pprint(resp)
        #
        print(f"GET @ /scan/{scan_id}/i3-event ...")
        resp = await rc.request(
            "GET", f"/scan/{scan_id}/i3-event", {"include_deleted": True}
        )
        pprint.pprint(resp)
        #
        print(f"GET @ /scan/{scan_id}/result ...")
        resp = await rc.request(
            "GET", f"/scan/{scan_id}/result", {"include_deleted": True}
        )
        pprint.pprint(resp)
        #
        print("\n---\n")

    print("\n---\n")
    pprint.pprint(versions)


# Run the asyncio event loop
if __name__ == "__main__":
    asyncio.run(main())
    print("Done.")
