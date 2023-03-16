"""For stopping Skymap Scanner clients on an HTCondor cluster."""


import argparse

from . import condor_tools
from .config import LOGGER


def attach_sub_parser_args(sub_parser: argparse.ArgumentParser) -> None:
    """Add args to subparser."""
    pass


def stop(args: argparse.Namespace) -> None:
    """Main logic."""
    LOGGER.info(
        f"Stopping Skymap Scanner client jobs on {args.collector} / {args.schedd}"
    )

    schedd_obj = condor_tools.get_schedd_obj(args.collector, args.schedd)
    jobs = []

    # Remove jobs -- may not be instantaneous
    schedd_obj.act(JobAction.Remove, jobs, reason="Requested by SkyDriver")
