"""The central module."""


import argparse

from wipac_dev_tools import logging_tools

from . import condor_tools, starter, stopper, utils
from .config import ENV, LOGGER


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
    starter.attach_sub_parser_args(subparsers.add_parser("start", help="start jobs"))
    stopper.attach_sub_parser_args(subparsers.add_parser("stop", help="stop jobs"))

    # parse args & set up logging
    args = parser.parse_args()
    logging_tools.set_level(
        "DEBUG",  # os.getenv("SKYSCAN_LOG", "INFO"),  # type: ignore[arg-type]
        first_party_loggers=LOGGER,
        third_party_level=ENV.SKYSCAN_LOG_THIRD_PARTY,  # type: ignore[arg-type]
        use_coloredlogs=True,  # for formatting
    )
    logging_tools.log_argparse_args(args, logger=LOGGER, level="WARNING")

    ####################################################################################

    # Go!
    match args.action:
        case "start":
            for i, (collector, schedd, njobs) in enumerate(args.cluster):
                LOGGER.info(
                    f"Starting {njobs} Skymap Scanner client jobs on {collector} / {schedd}"
                )
                # make connections -- do now so we don't have any surprises downstream
                skydriver_rc = utils.connect_to_skydriver()
                # start
                submit_result_obj = starter.start(
                    condor_tools.get_schedd_obj(collector, schedd),
                    njobs,
                    args.logs_directory / str(i) if args.logs_directory else None,
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
                    collector,
                    schedd,
                )
                LOGGER.info("Sent cluster info to SkyDriver")
            return
        case "stop":
            return stopper.stop(
                args,
                condor_tools.get_schedd_obj(args.collector, args.schedd),
            )
    raise RuntimeError(f"Unknown action: {args.action}")
