"""The EWMS Sidecar."""


import argparse
import time
from pathlib import Path

from wipac_dev_tools import argparse_tools, logging_tools

from . import condor
from .config import ENV, LOGGER


def main() -> None:
    """Main."""
    parser = argparse.ArgumentParser(
        description="Handle EWMS requests adjacent to a Skymap Scanner central server",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # method
    parser.add_argument(  # TODO - remove once EWMS is full-time
        dest="method",
        help="how to start up the jobs",
    )

    parser.add_argument(
        "--uuid",
        required=True,
        help="the uuid for the cluster",
    )

    SidecarArgs.condor(parser)
    SidecarArgs.starter(parser)

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

    # Go!
    match args.method:
        case "direct-remote-condor":
            condor.act(args)
        # case "ewms":
        #     ewms.act(args)
        case other:
            raise RuntimeError(f"method not supported: {other}")


class SidecarArgs:
    @staticmethod
    def condor(parser: argparse.ArgumentParser) -> None:
        """Add args to parser."""
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

    @staticmethod
    def starter(parser: argparse.ArgumentParser) -> None:
        """Add args to parser."""

        def wait_for_file(waitee: Path, wait_time: int) -> Path:
            """Wait for `waitee` to exist, then return fullly-resolved path."""
            elapsed_time = 0
            sleep = 5
            while not waitee.exists():
                LOGGER.info(f"waiting for {waitee} ({sleep}s intervals)...")
                time.sleep(sleep)
                elapsed_time += sleep
                if elapsed_time >= wait_time:
                    raise argparse.ArgumentTypeError(
                        f"FileNotFoundError: waited {wait_time}s [{waitee}]"
                    )
            return waitee.resolve()

        # helper args
        parser.add_argument(
            "--dryrun",
            default=False,
            action="store_true",
            help="does everything except submitting the worker(s)",
        )
        parser.add_argument(
            "--spool",
            default=False,
            action="store_true",
            help="whether to spool (persist) logs -- if not given, logs are not kept",
        )

        # worker args
        parser.add_argument(
            "--worker-memory-bytes",
            required=True,
            type=int,
            help="amount of worker memory (bytes)",
        )
        parser.add_argument(
            "--worker-disk-bytes",
            required=True,
            type=int,
            help="amount of worker disk (bytes)",
        )
        parser.add_argument(
            "--n-cores",
            default=1,
            type=int,
            help="number of cores per worker",
        )
        parser.add_argument(
            "--n-workers",
            required=True,
            type=int,
            help="number of worker to start",
        )
        parser.add_argument(
            "--max-worker-runtime",
            required=True,
            type=int,
            help="how long each worker is allowed to run",
        )
        parser.add_argument(
            "--priority",
            required=True,
            help="relative priority of this job/jobs",
        )

        # client args
        parser.add_argument(
            "--client-args",
            required=False,
            nargs="*",
            type=lambda x: argparse_tools.validate_arg(
                x.split(":", maxsplit=1),
                len(x.split(":", maxsplit=1)) == 2,
                ValueError('must " "-delimited series of "clientarg:value"-tuples'),
            ),
            help="n 'key:value' pairs containing the python CL arguments to pass to skymap_scanner.client",
        )
        parser.add_argument(
            "--client-startup-json",
            help="The 'startup.json' file to startup each client",
            type=lambda x: wait_for_file(
                Path(x),
                ENV.CLIENT_STARTER_WAIT_FOR_STARTUP_JSON,
            ),
        )
        parser.add_argument(
            "--image",
            required=True,
            help="a path or url to the workers' image",
        )
