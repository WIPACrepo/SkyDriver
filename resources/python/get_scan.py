"""Simple script to grab a scan's manifest.

Useful for quick debugging in prod.
"""

import argparse
import asyncio
import json
import logging

from rest_tools.client import RestClient, SavedDeviceGrantAuth

logging.getLogger().setLevel(logging.INFO)


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


async def main():
    parser = argparse.ArgumentParser(
        description="Get a scan's manifest",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "scan_id",
        help="the scan's id",
    )
    parser.add_argument(
        "--skydriver-url",
        required=True,
        help="the url to connect to a SkyDriver server",
    )
    args = parser.parse_args()

    rc = get_rest_client(args.skydriver_url)

    logging.info(f"getting manifest for scan {args.scan_id}")
    resp = await rc.request(
        "GET", f"/scan/{args.scan_id}/manifest", {"include_deleted": True}
    )
    print(json.dumps(resp, indent=4), flush=True)

    logging.info(f"getting statuses for scan {args.scan_id}")
    resp = await rc.request("GET", f"/scan/{args.scan_id}/status")
    print(json.dumps(resp, indent=4), flush=True)

    logging.info(f"getting logs for scan {args.scan_id}")
    resp = await rc.request("GET", f"/scan/{args.scan_id}/logs")
    print(json.dumps(resp, indent=4), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
    logging.info("Done.")
