"""For stopping Skymap Scanner clients on an HTCondor cluster."""


import argparse

import htcondor  # type: ignore[import]

from . import condor_tools
from .config import LOGGER


def attach_sub_parser_args(sub_parser: argparse.ArgumentParser) -> None:
    """Add args to subparser."""
    sub_parser.add_argument(
        "--cluster-id",
        required=True,
        help="the cluster id of the jobs to be stopped/removed",
    )


def stop(args: argparse.Namespace) -> None:
    """Main logic."""
    LOGGER.info(
        f"Stopping Skymap Scanner client jobs on {args.cluster_id} / {args.collector} / {args.schedd}"
    )

    # make connections -- do now so we don't have any surprises
    schedd_obj = condor_tools.get_schedd_obj(args.collector, args.schedd)
    # skydriver_rc, scan_id = utils.connect_to_skydriver()

    # Remove jobs -- may not be instantaneous
    LOGGER.info("Requesting removal...")
    schedd_obj.act(
        htcondor.JobAction.Remove,  # pylint:disable=no-member
        f"ClusterId == {args.cluster_id}",
        reason="Requested by SkyDriver",
    )

    # TODO: get/forward job logs
