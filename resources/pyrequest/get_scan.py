"""Simple script to grab one or more scan manifests, statuses, and logs.

Useful for quick debugging in prod.
"""

import argparse
import asyncio
import json
import logging
from typing import Any

from rest_tools.client import RestClient

from _connect import get_rest_client  # type: ignore[import-not-found]

logging.getLogger().setLevel(logging.INFO)


def extract_keys(obj: dict[str, Any], keys: list[str] | None) -> dict[str, Any]:
    """
    Extracts a subset of keys from a nested dictionary using dot notation.

    Args:
        obj: The full response object (a dict).
        keys: A list of dot-notated key paths to extract. If None or empty,
              the entire object is returned.

    Returns:
        A dictionary containing only the requested keys and their extracted values.
        If a key path is not found, it is included in the result with a placeholder
        string indicating it's missing.
    """
    if not keys:
        return obj
    result: dict[str, Any] = {}
    for key in keys:
        parts = key.split(".")
        current = obj
        try:
            for part in parts:
                current = current[part]
            result[key] = current
        except (KeyError, TypeError):
            result[key] = f"[missing: {key}]"
    return result


async def fetch_scan_info(
    rc: RestClient,
    scan_id: str,
    scan_request_keys: list[str] | None,
    manifest_keys: list[str] | None,
    status_keys: list[str] | None,
    log_keys: list[str] | None,
) -> None:
    logging.info(f"Processing scan {scan_id}")

    if scan_request_keys is not None:
        logging.info(f"Getting scan request object for scan {scan_id}")
        resp = await rc.request("GET", f"/scan-request/{scan_id}")
        subset = extract_keys(resp, scan_request_keys)
        print(f"\n=== Scan Request Object for scan {scan_id} ===")
        print(json.dumps(subset, indent=4), flush=True)
        print()

    if manifest_keys is not None:
        logging.info(f"Getting manifest for scan {scan_id}")
        resp = await rc.request(
            "GET", f"/scan/{scan_id}/manifest", {"include_deleted": True}
        )
        subset = extract_keys(resp, manifest_keys)
        print(f"\n=== Manifest for scan {scan_id} ===")
        print(json.dumps(subset, indent=4), flush=True)
        print()

    if status_keys is not None:
        logging.info(f"Getting status for scan {scan_id}")
        resp = await rc.request("GET", f"/scan/{scan_id}/status")
        subset = extract_keys(resp, status_keys)
        print(f"\n=== Status for scan {scan_id} ===")
        print(json.dumps(subset, indent=4), flush=True)
        print()

    if log_keys is not None:
        logging.info(f"Getting logs for scan {scan_id}")
        resp = await rc.request("GET", f"/scan/{scan_id}/logs")
        subset = extract_keys(resp, log_keys)
        print(f"\n=== Logs for scan {scan_id} ===")
        print(json.dumps(subset, indent=4), flush=True)
        print()


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Get data for one or more scans",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--scan-ids",
        nargs="+",
        required=True,
        help="One or more scan IDs",
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
    parser.add_argument(
        "--manifest",
        nargs="*",
        help=(
            "Optional keys to extract from the manifest response (dot notation "
            "allowed). Use without arguments to print full manifest."
        ),
    )
    parser.add_argument(
        "--status",
        nargs="*",
        help=(
            "Optional keys to extract from the status response (dot notation "
            "allowed). Use without arguments to print full status."
        ),
    )
    parser.add_argument(
        "--logs",
        nargs="*",
        help=(
            "Optional keys to extract from the logs response (dot notation "
            "allowed). Use without arguments to print full logs."
        ),
    )
    parser.add_argument(
        "--scan-request",
        nargs="*",
        help=(
            "Optional keys to extract from the scan request object (dot notation "
            "allowed). Use without arguments to print the full scan request object."
        ),
    )

    args = parser.parse_args()

    if not args.scan_ids:
        args.scan_ids = input("Scan ID(s): ").split()

    if all(
        v is None for v in (args.scan_request, args.manifest, args.status, args.logs)
    ):
        parser.error(
            "At least one of --scan-request, --manifest, --status, or --logs must be specified."
        )

    rc = get_rest_client(args.skydriver_type)

    for scan_id in args.scan_ids:
        await fetch_scan_info(
            rc,
            scan_id,
            args.scan_request,
            args.manifest,
            args.status,
            args.logs,
        )

    logging.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())
