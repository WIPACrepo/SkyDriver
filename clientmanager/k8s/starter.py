"""For starting Skymap Scanner clients on an HTCondor cluster."""


import argparse
import datetime as dt
import getpass
import json
import time
from pathlib import Path

import kubernetes  # type: ignore[import]
from wipac_dev_tools import argparse_tools

from . import condor_tools
from .config import ENV, LOGGER
from .utils import S3File

def _get_log_fpath(logs_subdir: Path) -> Path:
    return logs_subdir / "clientmanager.log"


def make_k8s_job_desc(  # pylint: disable=too-many-argument
    # k8s args
    namespace: str,
    name: str,
    memory: str,
    n_jobs: int,
    n_cores: int,
    # skymap scanner args
    container_image: str,
    client_startup_json_s3: S3File,
    add_client_args: list[tuple[str, str]],
    # special args for the cloud
    cpu_arch = "x64": str,

) -> dict:  # pylint:disable=no-member
    """Make the k8s job description (submit object)."""
    if (cpu_arch == "arm"):
        with open("k8s_job_arm_stub.json", "r") as f:
            k8s_job_dict = json.load(f)
    else:
        with open("k8s_job_stub.json", "r") as f:
            k8s_job_dict = json.load(f)
    # Setting namespace
    k8s_job_dict["metadata"]["namespace"] = namespace
    k8s_job_dict["metadata"]["name"] = name

    # Setting parallelism
    k8s_job_dict["spec"]["completions"] = n_jobs
    k8s_job_dict["spec"]["parallelism"] = n_jobs

    # Setting JSON input file
    k8s_job_dict["spec"]["template"]["spec"]["initContainers"][0]["env"][0]["value"] = client_startup_json_s3

    # Container image
    k8s_job_dict["spec"]["template"]["spec"]["containers"][0]["image"] = container_image

    # Adding more args to client
    client_args = k8s_job_dict["spec"]["template"]["spec"]["containers"][0]["args"]
    for carg, value in add_client_args:
        client_args.append(f"--{carg}")
        client_args.append(f"{value}")
    k8s_job_dict["spec"]["template"]["spec"]["containers"][0]["args"] = client_args

    return k8s_job_dict


def attach_sub_parser_args(sub_parser: argparse.ArgumentParser) -> None:
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
        help="does everything except submitting the k8s job(s)",
    )
    sub_parser.add_argument(
        "--logs-directory",
        default=None,
        type=Path,
        help="where to save logs (if not given, logs are not saved)",
    )
    # k8s args
    sub_parser.add_argument(
        "--n-jobs",
        required=True,
        type=int,
        help="number of jobs to start",
    )
    # Minimum memory
    sub_parser.add_argument(
        "--memory",
        required=True,
        help="amount of memory",
        # default="8GB",
    )
    # client args
    sub_parser.add_argument(
        "--container-image",
        required=True,
        help="a path or url to the OIC image",
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


def start(
    k8s_client: ,  # pylint:disable=no-member
    namespace: str,
    job_count: int,
    core_count: int,
    client_args: list[tuple[str, str]],
    memory: str,
    container_image: str,
    client_startup_json_s3: S3File,
    dryrun: bool,
    cpu_arch = "x64": str
) -> list:  # pylint:disable=no-member
    """Main logic."""

    # make k8s job description
    k8s_job_dict = make_k8s_job_desc(
        namespace,
        name,
        # condor args
        memory,
        job_count,
        core_count,
        # skymap scanner args
        container_image,
        client_startup_json_s3,
        client_args,
        cpu_arch
    )

    # LOGGER.info(k8s_job_dict)

    # dryrun?
    if dryrun:
        LOGGER.info(k8s_job_dict)
        LOGGER.error("Script Aborted: Condor job not submitted")
        return

    # submit
    kubernetes.utils.create_from_dict(k8s_client, k8s_job_dict, namespace=namespace)

    return k8s_result_obj
