"""For stopping Skymap Scanner clients on an HTCondor cluster."""


import argparse

import kubernetes  # type: ignore[import]

from .config import LOGGER


def attach_sub_parser_args(sub_parser: argparse.ArgumentParser) -> None:
    """Add args to subparser."""
    sub_parser.add_argument(
        "--job-name",
        required=True,
        help="the name of the jobs to be stopped/removed",
    )


def stop(args: argparse.Namespace, 
         k8s_client) -> None:
    """Main logic."""
    LOGGER.info(
        f"Stopping Skymap Scanner client jobs on {args.cluster_id} / {args.collector} / {args.schedd}"
    )

    # Remove jobs -- may not be instantaneous
    LOGGER.info("Requesting removal...")
    k8s_response = k8s_client.delete_namespaced_job(
        name=JOB_NAME,
        namespace="default",
        body=client.V1DeleteOptions(
            propagation_policy='Foreground',
            grace_period_seconds=5))
    LOGGER.debug("Job deleted. status='%s'" % str(k8s_response.status))
    LOGGER.info(f"Removed jobs: {args.job_name} in namespace {args.namespace} with response {k8s_response.status} ")

    # TODO: get/forward job logs
