"""The post-argparse entry point for k8s actions."""


import argparse
import time

import kubernetes  # type: ignore[import]

from .. import utils
from ..config import ENV, LOGGER
from . import starter, stopper


def act(args: argparse.Namespace, k8s_client: kubernetes.client.ApiClient) -> None:
    """Do the action."""
    match args.action:
        case "start":
            LOGGER.info(
                f"Starting {args.n_workers} Skymap Scanner client workers on {args.host} / {args.namespace}"
            )
            # make connections -- do now so we don't have any surprises downstream
            skydriver_rc = utils.connect_to_skydriver()
            # start
            cluster_id = f"{ENV.SKYSCAN_SKYDRIVER_SCAN_ID}-{int(time.time())}"  # TODO: make more unique
            starter.start(
                k8s_client=k8s_client,
                namespace=args.namespace,
                cluster_id=cluster_id,
                n_workers=args.n_workers,
                core_count=1,
                client_args=args.client_args,
                memory=args.memory,
                container_image=args.image,
                # put client_startup_json in S3 bucket
                client_startup_json_s3=utils.s3ify(args.client_startup_json),
                dryrun=args.dryrun,
            )
            # report to SkyDriver
            utils.update_skydriver(
                skydriver_rc,
                "k8s",
                location={
                    "host": args.host,
                    "namespace": args.namespace,
                },
                cluster_id=cluster_id,
                n_workers=args.n_workers,
            )
            LOGGER.info("Sent cluster info to SkyDriver")
        case "stop":
            stopper.stop(
                args.namespace,
                args.cluster_id,
                k8s_client,
            )
        case _:
            raise RuntimeError(f"Unknown action: {args.action}")
