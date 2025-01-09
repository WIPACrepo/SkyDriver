"""Run the EWMS Init Container logic."""

import argparse
import json
import logging
from pathlib import Path

LOGGER = logging.getLogger(__package__)


def get_workflow_id(scan_id: str) -> str:
    """Retrieve the workflow id for the scan (w/ `scan_id`)."""
    LOGGER.info(f"getting workflow id for scan {scan_id}...")


def get_ewms_attrs(workflow_id: str) -> dict[str, str]:
    """Retrieve the EWMS attributes for the workflow."""
    LOGGER.info(f"getting EWMS attributes for workflow {workflow_id}...")


def _assure_json(val: str) -> Path:
    fpath = Path(val)
    if fpath.suffix != ".json":
        raise ValueError(f"File {fpath} is not a JSON file.")
    return fpath


def main() -> None:
    """Main."""
    parser = argparse.ArgumentParser(
        description="Retrieve EWMS attributes for use by a Skymap Scanner instance.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "scan_id",
        type=str,
        help="the scan id",
    )
    parser.add_argument(
        "--json-out",
        type=_assure_json,
        help="the json file to write the map of EWMS attributes to",
    )
    args = parser.parse_args()

    workflow_id = get_workflow_id(args.scan_id)
    ewms_dict = get_ewms_attrs(workflow_id)

    LOGGER.info(f"dumping EWMS attributes to '{args.json_out}'...")
    with open(args.json_out, "w") as f:
        json.dump(ewms_dict, f)


if __name__ == "__main__":
    main()
    LOGGER.info("Done.")
