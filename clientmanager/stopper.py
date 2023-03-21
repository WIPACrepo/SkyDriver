"""For stopping Skymap Scanner clients on an HTCondor cluster."""


import argparse

import htcondor  # type: ignore[import]

from .config import LOGGER


def attach_sub_parser_args(sub_parser: argparse.ArgumentParser) -> None:
    """Add args to subparser."""
    sub_parser.add_argument(
        "--",
        required=True,
        help="the ",
    )
    sub_parser.add_argument(
        "--cluster-id",
        nargs="+",
        type=int,
        help=(
            "the cluster id of the jobs to be stopped/removed for each cluster (collector/schedd), "
            "this will be a series of ids if providing multiple clusters. "
            "Each id will be assigned to the matching order of '--cluster' entries"
        ),
    )


def stop(
    schedd_obj: htcondor.Schedd,  # pylint:disable=no-member
    cluster_id: str,
) -> None:
    """Main logic."""

    # Remove jobs -- may not be instantaneous
    LOGGER.info(f"Requesting removal on Cluster {cluster_id}...")
    act_obj = schedd_obj.act(
        htcondor.JobAction.Remove,  # pylint:disable=no-member
        f"ClusterId == {cluster_id}",
        reason="Requested by SkyDriver",
    )
    LOGGER.debug(act_obj)
    LOGGER.info(f"Removed {act_obj['TotalSuccess']} jobs")

    # TODO: get/forward job logs
