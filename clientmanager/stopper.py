"""For stopping Skymap Scanner clients on an HTCondor cluster."""


import argparse

import htcondor  # type: ignore[import]

from .config import LOGGER


def attach_sub_parser_args(sub_parser: argparse.ArgumentParser) -> None:
    """Add args to subparser."""
    sub_parser.add_argument(
        "--cluster-id",
        required=True,
        help="the cluster id of the jobs to be stopped/removed",
    )


def stop(args: argparse.Namespace, schedd_obj: htcondor.Schedd) -> None:
    """Main logic."""
    LOGGER.info(
        f"Stopping Skymap Scanner client jobs on {args.cluster_id} / {args.collector} / {args.schedd}"
    )

    # Remove jobs -- may not be instantaneous
    LOGGER.info("Requesting removal...")
    act_obj = schedd_obj.act(
        htcondor.JobAction.Remove,  # pylint:disable=no-member
        f"ClusterId == {args.cluster_id}",
        reason="Requested by SkyDriver",
    )
    LOGGER.debug(act_obj)
    LOGGER.info(f"Removed {act_obj['TotalSuccess']} jobs")

    # TODO: get/forward job logs
