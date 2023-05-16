"""The central module."""


import argparse

import htcondor  # type: ignore[import]
from wipac_dev_tools import logging_tools

from . import condor_tools, starter, stopper, utils
from ..config import ENV, LOGGER


def main() -> None:
    """Main."""
    parser = argparse.ArgumentParser(
        description="Manage Skymap Scanner clients as HTCondor jobs",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(
        required=True,
        dest="action",
        help="clientmanager action",
    )
    parser.add_argument(
        "--collector",
        default="",
        help="the full URL address of the HTCondor collector server. Ex: foo-bar.icecube.wisc.edu",
    )
    parser.add_argument(
        "--schedd",
        default="",
        help="the full DNS name of the HTCondor Schedd server. Ex: baz.icecube.wisc.edu",
    )
    starter.attach_sub_parser_args(subparsers.add_parser("start", help="start jobs"))
    stopper.attach_sub_parser_args(subparsers.add_parser("stop", help="stop jobs"))

    # parse args & set up logging
    args = parser.parse_args()
    logging_tools.set_level(
        "DEBUG",  # os.getenv("SKYSCAN_LOG", "INFO"),  # type: ignore[arg-type]
        first_party_loggers=LOGGER,
        third_party_level=ENV.SKYSCAN_LOG_THIRD_PARTY,  # type: ignore[arg-type]
        use_coloredlogs=True,  # for formatting
        future_third_parties=["boto3", "botocore"],
    )
    logging_tools.log_argparse_args(args, logger=LOGGER, level="WARNING")

    ####################################################################################

    # Go!
    with htcondor.SecMan() as secman:
        secman.setToken(htcondor.Token(ENV.CONDOR_TOKEN))
        schedd_obj = condor_tools.get_schedd_obj(args.collector, args.schedd)
        act(args, schedd_obj)


def act(args: argparse.Namespace, schedd_obj: htcondor.Schedd) -> None:
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
