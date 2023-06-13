"""The post-argparse entry point for condor actions."""


import argparse

import htcondor  # type: ignore[import]

from .. import utils
from ..config import LOGGER
from . import starter, stopper


def act(
    args: argparse.Namespace,
    schedd_obj: htcondor.Schedd,  # pylint:disable=no-member
) -> None:
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
                schedd_obj,
                args.n_jobs,
                args.logs_directory if args.logs_directory else None,
                args.client_args,
                args.memory,
                args.accounting_group,
                args.singularity_image,
                # put client_startup_json in S3 bucket
                utils.s3ify(args.client_startup_json),
                args.dryrun,
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
                schedd_obj,
            )
        case _:
            raise RuntimeError(f"Unknown action: {args.action}")
