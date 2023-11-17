"""The central module."""


import argparse
import time
from pathlib import Path

from wipac_dev_tools import argparse_tools, logging_tools

from . import condor, k8s
from .config import ENV, LOGGER


def main() -> None:
    """Main."""
    parser = argparse.ArgumentParser(
        description="Manage Skymap Scanner client workers",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--uuid",
        required=True,
        help="the uuid for the cluster",
    )

    # orchestrator
    orch_subparsers = parser.add_subparsers(
        required=True,
        dest="orchestrator",
        help="the resource orchestration tool to use for worker scheduling",
    )
    OrchestratorArgs.condor(
        orch_condor_parser := orch_subparsers.add_parser(
            "condor", help="orchestrate with HTCondor"
        )
    )
    OrchestratorArgs.k8s(
        orch_k8s_parser := orch_subparsers.add_parser(
            "k8s", help="orchestrate with Kubernetes"
        )
    )

    # action -- add sub-parser to each sub-parser (can't add multiple sub-parsers)
    for p in [orch_condor_parser, orch_k8s_parser]:
        act_subparsers = p.add_subparsers(
            required=True,
            dest="action",
            help="the action to perform on the worker cluster",
        )
        ActionArgs.starter(act_subparsers.add_parser("start", help="start workers"))
        ActionArgs.stopper(act_subparsers.add_parser("stop", help="stop workers"))

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
    match args.orchestrator:
        case "condor":
            condor.act(args)
        case "k8s":
            k8s.act(args)
        case other:
            raise RuntimeError(f"Orchestrator not supported: {other}")


class OrchestratorArgs:
    @staticmethod
    def condor(sub_parser: argparse.ArgumentParser) -> None:
        """Add args to subparser."""
        sub_parser.add_argument(
            "--collector",
            default="",
            help="the full URL address of the HTCondor collector server. Ex: foo-bar.icecube.wisc.edu",
        )
        sub_parser.add_argument(
            "--schedd",
            default="",
            help="the full DNS name of the HTCondor Schedd server. Ex: baz.icecube.wisc.edu",
        )

    @staticmethod
    def k8s(sub_parser: argparse.ArgumentParser) -> None:
        """Add args to subparser."""
        sub_parser.add_argument(
            "--host",
            required=True,
            help="the host server address to connect to for running workers",
        )
        sub_parser.add_argument(
            "--namespace",
            required=True,
            help="the k8s namespace to use for running workers",
        )
        sub_parser.add_argument(
            "--cpu-arch",
            default="x64",
            help="which CPU architecture to use for running workers",
        )
        sub_parser.add_argument(
            "--job-config-stub",
            type=Path,
            default=Path("resources/worker_k8s_job_stub.json"),
            help="worker k8s job config file to dynamically complete, then run (json)",
        )


class ActionArgs:
    @staticmethod
    def starter(sub_parser: argparse.ArgumentParser) -> None:
        """Add args to subparser."""

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
        sub_parser.add_argument(
            "--dryrun",
            default=False,
            action="store_true",
            help="does everything except submitting the worker(s)",
        )
        sub_parser.add_argument(
            "--spool-logs-directory",
            default=None,
            type=Path,
            help="where to spool (persist) logs -- if not given, logs are not kept",
        )

        # worker args
        sub_parser.add_argument(
            "--memory",
            required=True,
            help="amount of memory",
            # default="8GB",
        )
        sub_parser.add_argument(
            "--n-cores",
            default=1,
            type=int,
            help="number of cores per worker",
        )
        sub_parser.add_argument(
            "--n-workers",
            required=True,
            type=int,
            help="number of worker to start",
        )
        sub_parser.add_argument(
            "--execution-time-limit",
            default=4 * 60 * 60,  # TODO - determine who is responsible for setting this
            type=int,
            help="how long each worker is allowed to run -- condor only",  # TODO - set for k8s?
        )

        # client args
        sub_parser.add_argument(
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
        sub_parser.add_argument(
            "--client-startup-json",
            help="The 'startup.json' file to startup each client",
            type=lambda x: wait_for_file(
                Path(x),
                ENV.CLIENT_STARTER_WAIT_FOR_STARTUP_JSON,
            ),
        )
        sub_parser.add_argument(
            "--image",
            required=True,
            help="a path or url to the workers' image",
        )

    @staticmethod
    def stopper(sub_parser: argparse.ArgumentParser) -> None:
        """Add args to subparser."""
        sub_parser.add_argument(
            "--cluster-id",
            required=True,
            help="the cluster id of the workers to be stopped/removed",
        )
