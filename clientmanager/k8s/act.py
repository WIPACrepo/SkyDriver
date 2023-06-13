"""The post-argparse entry point for k8s actions."""


import argparse

import kubernetes  # type: ignore[import]

from .. import utils
from ..config import ENV, LOGGER
from . import starter, stopper


def act(args: argparse.Namespace, k8s_client: kubernetes.client.ApiClient) -> None:
    """Do the action."""
    match args.action:
        case "start":
            LOGGER.info(
                f"Starting {args.n_jobs} Skymap Scanner client jobs on {args.collector} / {args.schedd}"
            )
            # make connections -- do now so we don't have any surprises downstream
            skydriver_rc = utils.connect_to_skydriver()
            # start
            k8s_job_dict = starter.start(
                k8s_client,
                ENV.WORKER_K8S_NAMESPACE,
                ENV.SKYSCAN_SKYDRIVER_SCAN_ID,
                args.name,
                args.n_jobs,
                args.client_args,
                args.memory,
                args.singularity_image,
                # put client_startup_json in S3 bucket
                utils.s3ify(args.client_startup_json),
                args.dryrun,
            )
            # report to SkyDriver
            utils.update_skydriver(
                skydriver_rc,
                k8s_job_dict,
                args.collector,
                args.schedd,
            )
            LOGGER.info("Sent cluster info to SkyDriver")
        case "stop":
            stopper.stop(
                ENV.WORKER_K8S_NAMESPACE,
                ENV.SKYSCAN_SKYDRIVER_SCAN_ID,
                k8s_client,
            )
        case _:
            raise RuntimeError(f"Unknown action: {args.action}")
