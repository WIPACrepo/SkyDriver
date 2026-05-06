"""A script to randomly query SkyDriver.

This will makes sure everything that should be accessible is accessible.
"""

import argparse
import asyncio
import logging
import random
from collections import defaultdict
from pathlib import Path

import rich.console
import rich.live
import rich.panel
import rich.progress
import wipac_dev_tools
from rest_tools.client import RestClient, SavedDeviceGrantAuth

CHUNK_SIZE = 50


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


async def _log_and_request(
    rc: RestClient,
    method: str,
    path: str,
    params: dict,
    console: rich.console.Console,
) -> dict:
    console.print(f"{method} @ {path}")  # print in case query fails
    out = await rc.request(method, path, params)
    console.print(out)
    return out


# Function to split list into chunks
def chunk_list(data, size):
    for i in range(0, len(data), size):
        yield data[i : i + size]


async def main():  # noqa: PLR0915
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
    parser.add_argument(
        "--sample",
        dest="sample_pct",
        default=None,
        type=lambda x: wipac_dev_tools.argparse_tools.validate_arg(
            float(x),
            0.01 <= float(x) <= 0.99,
            ValueError("--sample must be between 0.01 and 0.99"),
        ),
        help="sample a fraction of the scans (0.01 = 1 percent)",
    )
    args = parser.parse_args()

    rc = get_rest_client(args.skydriver_type)

    # terminal UI
    console = rich.console.Console()
    p = rich.progress.Progress(
        rich.progress.SpinnerColumn(),
        rich.progress.TextColumn("[bold blue]{task.description}"),
        rich.progress.BarColumn(),
        rich.progress.TaskProgressColumn(),
        rich.progress.MofNCompleteColumn(),
        rich.progress.TimeElapsedColumn(),
    )
    sample_notice = rich.panel.Panel(
        (
            f"[dim]Using: [bold white]--sample {args.sample_pct}[/bold white] to query fewer scans and run faster[/dim]"
            if args.sample_pct
            else "[dim]Tip: pass [bold white]--sample N[/bold white] to query fewer scans and run faster[/dim]"
        ),
        style="dim",
    )

    with rich.live.Live(
        rich.console.Group(p, sample_notice), console=console, refresh_per_second=10
    ):
        ################################################################################
        # 1: get all the scan_ids (not too large)
        ################################################################################
        p_scans_find_ids = p.add_task(
            "Getting all scan ids — POST @ /scans/find", total=None
        )
        scan_ids = [
            m["scan_id"]
            for m in (
                await _log_and_request(
                    rc,
                    "POST",
                    "/scans/find",
                    {
                        "filter": {},
                        "include_deleted": True,
                        "manifest_projection": ["scan_id"],
                    },
                    console,
                )
            )["manifests"]
        ]
        if args.sample_pct:
            scan_ids = random.sample(
                scan_ids, max(1, int(len(scan_ids) * args.sample_pct))
            )
        else:
            random.shuffle(scan_ids)  # mix them up so successive runs are not identical
        p.update(p_scans_find_ids, total=1, completed=1)  # snaps to 100%

        console.rule()

        ################################################################################
        # 2: /scans/find - paginated
        ################################################################################
        total = 0
        versions = defaultdict(list)
        scan_id_pages = list(chunk_list(scan_ids, CHUNK_SIZE))
        p_scans_find_paginated = p.add_task(
            f"Requesting full manifests in bulk ({CHUNK_SIZE} scans at a time) — POST @ /scans/find",
            total=len(scan_id_pages),
        )
        for page in scan_id_pages:
            manifests = (
                await _log_and_request(
                    rc,
                    "POST",
                    "/scans/find",
                    {
                        "filter": {"scan_id": {"$in": page}},
                        "include_deleted": True,
                    },
                    console,
                )
            )["manifests"]
            console.print(f"found {len(manifests)}/{len(page)} scans (subset)")
            total += len(manifests)
            for m in manifests:
                if m.get("i3_event_id"):
                    versions[">= v1.2"].append(m["scan_id"])
                elif "i3_event_id" not in m:
                    versions["< v1.2"].append(m["scan_id"])
                else:
                    versions["other"].append(m["scan_id"])
            p.advance(p_scans_find_paginated)

        # print debug info -- in case of failures
        console.print(versions)
        console.print(f"confirmed {total} scans")
        assert total == len(scan_ids)
        # check that all versions are represented
        assert all(v for v in versions.values())

        console.rule()

        ################################################################################
        # 3. quickly query the backlog
        ################################################################################
        p_scans_backlog = p.add_task(
            "Looking at backlog — GET @ /scans/backlog", total=None
        )
        await _log_and_request(
            rc,
            "GET",
            "/scans/backlog",
            {},
            console,
        )
        p.update(p_scans_backlog, total=1, completed=1)  # snaps to 100%

        console.rule()

        ################################################################################
        # 4. query each scan
        ################################################################################
        p_scan_indiv = p.add_task("Requesting scans individually", total=len(scan_ids))
        _prefix = " — GET @ /scan/SCAN_ID"
        p_scan_indiv_breakdown = {
            "scan": p.add_task(_prefix, total=len(scan_ids)),
            "manifest": p.add_task(f"{_prefix}/manifest", total=len(scan_ids)),
            "i3-event": p.add_task(f"{_prefix}/i3-event", total=len(scan_ids)),
            "result": p.add_task(f"{_prefix}/result", total=len(scan_ids)),
        }
        for scan_id in scan_ids:
            # manifest
            await _log_and_request(
                rc,
                "GET",
                f"/scan/{scan_id}",
                {"include_deleted": True},
                console,
            )
            p.advance(p_scan_indiv_breakdown["scan"])
            #
            # manifest - in full
            await _log_and_request(
                rc,
                "GET",
                f"/scan/{scan_id}/manifest",
                {"include_deleted": True},
                console,
            )
            p.advance(p_scan_indiv_breakdown["manifest"])
            #
            # i3-event
            await _log_and_request(
                rc,
                "GET",
                f"/scan/{scan_id}/i3-event",
                {"include_deleted": True},
                console,
            )
            p.advance(p_scan_indiv_breakdown["i3-event"])
            #
            # result
            await _log_and_request(
                rc,
                "GET",
                f"/scan/{scan_id}/result",
                {"include_deleted": True},
                console,
            )
            p.advance(p_scan_indiv_breakdown["result"])
            #
            #
            console.rule()
            p.advance(p_scan_indiv)

        ################################################################################
        # Done
        ################################################################################
        console.rule()
        console.print(versions)
        console.print(f"confirmed {total} scans")


# Run the asyncio event loop
if __name__ == "__main__":
    asyncio.run(main())
    print("Done.")
