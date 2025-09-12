"""Helper to GET a stored scan-request, reuse its i3_event_id, apply overrides, and launch a new scan.

Usage example:
    python remix_scan.py <scan_id> --skydriver prod \
        --override reco_algo=new-algo \
        --override docker_tag=latest \
        --override nsides='{"1":2,"3":4}'

All overrides are passed directly to POST @ '/scan'. No local validation is done.
"""

import argparse
import asyncio
import json
import logging
from typing import Any

from _connect import get_rest_client  # type: ignore[import-not-found]

logging.getLogger().setLevel(logging.INFO)


def init_new_scanreq(override_list: list[str]) -> dict[str, Any]:
    """Apply user overrides (simple KEY=VALUE, try JSON parse fallback)."""
    body = {}

    for entry in override_list:
        if "=" not in entry:
            raise SystemExit(f"Bad override '{entry}' (must be KEY=VALUE)")
        key, val = entry.split("=", 1)
        try:
            body[key] = json.loads(val)
        except Exception:
            body[key] = val

    return body


async def main():
    parser = argparse.ArgumentParser(
        description="GET a stored scan-request, reuse i3_event_id, override fields, and POST a new scan",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "scan_id",
        help="existing scan_id whose stored scan-request will be used as a template",
    )
    parser.add_argument(
        "--skydriver",
        dest="skydriver_type",
        required=True,
        choices=["dev", "prod"],
        help="SkyDriver environment",
    )
    parser.add_argument(
        "--override",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="override/additional POST fields, e.g. --override reco_algo=new",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print body only",
    )
    args = parser.parse_args()

    rc = get_rest_client(args.skydriver_type)

    # get source scan request object
    logging.info(f"Fetching stored request for scan_id={args.scan_id}")
    source = await rc.request("GET", f"/scan-request/{args.scan_id}")
    # -- make edits
    new_scanreq = init_new_scanreq(args.override)
    new_scanreq["i3_event_id"] = source["i3_event_id"]
    logging.info("Constructed POST '/scan' body:")
    print(json.dumps(new_scanreq, indent=4), flush=True)

    # dry run?
    if args.dry_run:
        logging.info("Dry-run enabled; not posting.")
        return

    # launch new scan
    logging.info("Launching new scan...")
    resp = await rc.request("POST", "/scan", new_scanreq)
    print(json.dumps(resp, indent=4), flush=True)

    # log
    print("\nSummary:", flush=True)
    print(f"  source scan_id:     {args.scan_id}", flush=True)
    print(f"  new scan_id:        {resp.get('scan_id')}", flush=True)
    print(f"  reused i3_event_id: {resp.get('i3_event_id')}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
    logging.info("Done.")
