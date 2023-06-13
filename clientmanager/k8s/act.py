"""The post-argparse entry point for k8s actions."""


import argparse

import kubernetes  # type: ignore[import]

from .. import utils
from ..config import LOGGER
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
            submit_result_obj = starter.start(
                k8s_client,
                args.namespace,
                args.name,
                args.n_jobs,
                args.client_args,
                args.memory,
                args.accounting_group,
                args.singularity_image,
                # put client_startup_json in S3 bucket
                utils.s3ify(args.client_startup_json),
            )
            # report to SkyDriver
            utils.update_skydriver(
                skydriver_rc,
                submit_result_obj,
                args.collector,
                args.schedd,
            )
            LOGGER.info("Sent cluster info to SkyDriver")
        case "stop":
            stopper.stop(
                args,
                k8s_client,
            )
        case _:
            raise RuntimeError(f"Unknown action: {args.action}")
