"""The central module."""


import argparse
import os

from wipac_dev_tools import logging_tools

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
        "--collector",
        default="",
        help="the full URL address of the HTCondor collector server. Ex: foo-bar.icecube.wisc.edu",
    )
    parser.add_argument(
        "--schedd",
        default="",
        help="the full DNS name of the HTCondor Schedd server. Ex: baz.icecube.wisc.edu",
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
    condor_tools.condor_token_auth()
    schedd_obj = condor_tools.get_schedd_obj(args.collector, args.schedd)

    # Go!
    match args.action:
        case "start":
            return starter.start(args, skydriver_rc, scan_id, schedd_obj)
        case "stop":
            return stopper.stop(args, schedd_obj)
    raise RuntimeError(f"Unknown action: {args.action}")
