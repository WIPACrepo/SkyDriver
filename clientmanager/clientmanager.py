"""The central module."""


import argparse
import os

from wipac_dev_tools import argparse_tools, logging_tools

from . import condor_tools, starter, stopper, utils
from .config import LOGGER


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

    # common arguments
    parser.add_argument(
        "--cluster",
        default=[None, None],  # list of a single 2-list
        nargs="*",
        type=lambda x: argparse_tools.validate_arg(
            x.split(","),
            len(x.split(",")) == 2,
            ValueError('must " "-delimited series of "collector,schedd"-tuples'),
        ),
        help=(
            "the HTCondor clusters to use, each entry contains: "
            "full DNS name of Collector server, full DNS name of Schedd server"
            "Ex: foo-bar.icecube.wisc.edu,baz.icecube.wisc.edu alpha.icecube.wisc.edu,beta.icecube.wisc.edu"
        ),
    )

    # parse args & set up logging
    args = parser.parse_args()
    logging_tools.set_level(
        "DEBUG",  # os.getenv("SKYSCAN_LOG", "INFO"),  # type: ignore[arg-type]
        first_party_loggers=LOGGER,
        third_party_level=os.getenv("SKYSCAN_LOG_THIRD_PARTY", "WARNING"),  # type: ignore[arg-type]
        use_coloredlogs=True,  # for formatting
    )
    logging_tools.log_argparse_args(args, logger=LOGGER, level="WARNING")

    # make connections -- do now so we don't have any surprises downstream
    skydriver_rc, scan_id = utils.connect_to_skydriver()

    for i, (collector, schedd) in enumerate(args.clusters):
        if collector and schedd:
            condor_tools.condor_token_auth(collector, schedd)
            schedd_obj = condor_tools.get_schedd_obj(collector, schedd)

        # Go!
        match args.action:
            case "start":
                LOGGER.info(
                    f"Starting Skymap Scanner client jobs on {collector} / {schedd}"
                )
                return starter.start(
                    skydriver_rc,
                    scan_id,
                    schedd_obj,
                    args.jobs[i],
                    args.logs_directory / str(i),
                    args.client_args,
                    args.memory,
                    args.accounting_group,
                    args.singularity_image,
                    args.client_startup_json,
                    args.dryrun,
                    collector,
                    schedd,
                )
            case "stop":
                LOGGER.info(
                    f"Stopping Skymap Scanner client jobs on {collector} / {schedd}"
                )
                return stopper.stop(
                    schedd_obj,
                    args.cluster_id[i],
                )
        raise RuntimeError(f"Unknown action: {args.action}")
