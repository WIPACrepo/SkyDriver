"""For stopping Skymap Scanner clients on an HTCondor cluster."""


import argparse
from typing import Any

import htcondor  # type: ignore[import]
from rest_tools.client import RestClient

from . import condor_tools, utils
from .config import LOGGER


def attach_sub_parser_args(sub_parser: argparse.ArgumentParser) -> None:
    """Add args to subparser."""
    sub_parser.add_argument(
        "--cluster-id",
        required=True,
        help="the cluster id of the jobs to be stopped/removed",
    )


def get_cluster_info(
    skydriver_rc: RestClient,
    scan_id: str,
    collector: str,
    schedd: str,
    cluster_id: int,
) -> dict[str, Any]:
    """Get info from SkyDriver describing the condor cluster."""
    resp = skydriver_rc.request_seq("GET", f"/scan/manifest/{scan_id}")
    for cluster in resp["condor_clusters"]:
        if (
            cluster["cluster_id"] == cluster_id
            and cluster["schedd"] == schedd
            and cluster["collector"] == collector
        ):
            return cluster  # type: ignore[no-any-return]
    raise ValueError(f"Cluster Not Found: {cluster_id=}, {schedd=}, {collector=}")


def stop(args: argparse.Namespace) -> None:
    """Main logic."""
    LOGGER.info(
        f"Stopping Skymap Scanner client jobs on {args.cluster_id} / {args.collector} / {args.schedd}"
    )

    # make connections -- do now so we don't have any surprises
    schedd_obj = condor_tools.get_schedd_obj(args.collector, args.schedd)
    skydriver_rc, scan_id = utils.connect_to_skydriver()
    if not skydriver_rc:
        raise RuntimeError("There must be a connection to SkyDriver to stop jobs.")

    # get jobs (reconstruct 'htcondor.Submit' object)
    cluster = get_cluster_info(
        skydriver_rc,
        scan_id,
        args.collector,
        args.schedd,
        args.cluster_id,
    )
    submit_obj = htcondor.Submit(cluster["submit_dict"])  # pylint:disable=no-member
    jobs = condor_tools.get_job_classads(submit_obj, cluster["jobs"], args.cluster_id)

    # Remove jobs -- may not be instantaneous
    LOGGER.info("Requesting removal...")
    schedd_obj.act(
        htcondor.JobAction.Remove,  # pylint:disable=no-member
        jobs,
        reason="Requested by SkyDriver",
    )

    # TODO: get/forward job logs
