"""For starting Skymap Scanner clients on an HTCondor cluster."""


import argparse
import datetime as dt
import getpass
import os
import time
from pathlib import Path

import htcondor  # type: ignore[import]
from rest_tools.client import RestClient
from wipac_dev_tools import argparse_tools

from . import condor_tools
from .config import LOGGER


def make_condor_logs_subdir(directory: Path) -> Path:
    """Make the condor logs subdirectory."""
    iso_now = dt.datetime.now().isoformat(timespec="seconds")
    subdir = directory / f"skyscan-{iso_now}"
    subdir.mkdir(parents=True)
    LOGGER.info(f"HTCondor will write log files to {subdir}")
    return subdir


def _get_log_fpath(logs_subdir: Path) -> Path:
    return logs_subdir / "clientmanager.log"


def make_condor_job_description(  # pylint: disable=too-many-arguments
    logs_subdir: Path,
    # condor args
    memory: str,
    accounting_group: str,
    # skymap scanner args
    singularity_image: str,
    client_startup_json: Path,
    client_args_string: str,
) -> htcondor.Submit:  # pylint:disable=no-member
    """Make the condor job description (submit object)."""
    transfer_input_files: list[Path] = [client_startup_json]

    # NOTE:
    # In the newest version of condor we could use:
    #   universe = container
    #   container_image = ...
    #   arguments = python -m ...
    # But for now, we're stuck with:
    #   executable = ...
    #   +SingularityImage = ...
    #   arguments = /usr/local/icetray/env-shell.sh python -m ...
    # Because "this universe doesn't know how to do the
    #   entrypoint, and loading the icetray env file
    #   directly from cvmfs messes up the paths" -DS

    # Build the environment specification for condor
    env_vars = []
    # RABBITMQ_* and EWMS_* are inherited via condor `getenv`, but we have default in case these are not set.
    if not os.getenv("RABBITMQ_HEARTBEAT"):
        env_vars.append("RABBITMQ_HEARTBEAT=600")
    if not os.getenv("EWMS_PILOT_QUARANTINE_TIME"):
        env_vars.append("EWMS_PILOT_QUARANTINE_TIME=1800")
    # The container sets I3_DATA to /opt/i3-data, however `millipede_wilks` requires files (spline tables) that are not available in the image. For the time being we require CVFMS and we load I3_DATA from there. In order to override the environment variables we need to prepend APPTAINERENV_ or SINGULARITYENV_ to the variable name. There are site-dependent behaviour but these two should cover all cases. See https://github.com/icecube/skymap_scanner/issues/135#issuecomment-1449063054.
    for prefix in ["APPTAINERENV_", "SINGULARITYENV_"]:
        env_vars.append(f"{prefix}I3_DATA=/cvmfs/icecube.opensciencegrid.org/data")
    environment = " ".join(env_vars)

    # write
    submit_dict = {
        "executable": "/bin/bash",
        "arguments": f"/usr/local/icetray/env-shell.sh python -m skymap_scanner.client {client_args_string} --client-startup-json ./{client_startup_json.name}",
        "+SingularityImage": f'"{singularity_image}"',  # must be quoted
        "Requirements": "HAS_CVMFS_icecube_opensciencegrid_org && has_avx",
        "getenv": "SKYSCAN_*, EWMS_*, RABBITMQ_*, PULSAR_UNACKED_MESSAGES_TIMEOUT_SEC",
        "output": str(logs_subdir / "client-$(ProcId).out"),
        "environment": f'"{environment}"',  # must be quoted
        "error": str(logs_subdir / "client-$(ProcId).err"),
        "log": str(_get_log_fpath(logs_subdir)),
        "+FileSystemDomain": '"blah"',  # must be quoted
        "should_transfer_files": "YES",
        "transfer_input_files": ",".join(
            [os.path.abspath(str(f)) for f in transfer_input_files]
        ),
        "request_cpus": "1",
        "request_memory": memory,
        "notification": "Error",
    }

    # accounting group
    if accounting_group:
        submit_dict["+AccountingGroup"] = f"{accounting_group}.{getpass.getuser()}"

    return htcondor.Submit(submit_dict)  # pylint:disable=no-member


def update_skydriver(
    skydriver_rc: RestClient,
    scan_id: str,
    submit_result: htcondor.SubmitResult,  # pylint:disable=no-member
    collector: str,
    schedd: str,
) -> None:
    """Send SkyDriver updates from the `submit_result`."""
    skydriver_rc.request_seq(
        "PATCH",
        f"/scan/manifest/{scan_id}",
        {
            "condor_cluster": {
                "collector": collector,
                "schedd": schedd,
                "cluster_id": submit_result.cluster(),
                "jobs": submit_result.num_procs(),
            }
        },
    )


def attach_sub_parser_args(sub_parser: argparse.ArgumentParser) -> None:
    """Add args to subparser."""

    sub_parser.add_argument(
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
        help="does everything except submitting the condor job(s)",
    )
    sub_parser.add_argument(
        "--logs-directory",
        required=True,
        type=Path,
        help="where to save logs",
    )

    # condor args
    sub_parser.add_argument(
        "--accounting-group",
        default="",
        help=(
            "the accounting group to use, ex: 1_week. "
            "By default no accounting group is used."
        ),
    )
    sub_parser.add_argument(
        "--jobs",
        nargs="+",
        type=int,
        help=(
            "the number of jobs for each cluster (collector/schedd), "
            "this will be a series of numbers if providing multiple clusters. "
            "Each job # will be assigned to the matching order of '--cluster' entries"
        ),
    )
    sub_parser.add_argument(
        "--memory",
        required=True,
        help="amount of memory",
        # default="8GB",
    )

    # client args
    sub_parser.add_argument(
        "--singularity-image",
        required=True,
        help="a path or url to the singularity image",
    )
    sub_parser.add_argument(
        "--client-startup-json",
        help="The 'startup.json' file to startup each client",
        type=lambda x: wait_for_file(
            Path(x), int(os.getenv("CLIENT_STARTER_WAIT_FOR_STARTUP_JSON", "60"))
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
    skydriver_rc: RestClient | None,
    scan_id: str,
    schedd_obj: htcondor.Schedd,  # pylint:disable=no-member
    job_count: int,
    logs_directory: Path,
    client_args: list[tuple[str, str]],
    memory: str,
    accounting_group: str,
    singularity_image: str,
    client_startup_json: Path,
    dryrun: bool,
    collector: str,
    schedd: str,
) -> None:
    """Main logic."""
    logs_subdir = make_condor_logs_subdir(logs_directory)

    # get client args
    client_args_string = ""
    if client_args:
        for carg, value in client_args:
            client_args_string += f" --{carg} {value} "
        LOGGER.info(f"Client Args: {client_args}")
        if "--client-startup-json" in client_args_string:
            raise RuntimeError(
                "The '--client-args' arg cannot include \"--client-startup-json\". "
                "This needs to be given to this script explicitly ('--client-startup-json')."
            )

    # make condor job description
    submit_obj = make_condor_job_description(
        logs_subdir,
        # condor args
        memory,
        accounting_group,
        # skymap scanner args
        singularity_image,
        client_startup_json,
        client_args_string,
    )
    LOGGER.info(submit_obj)

    # dryrun?
    if dryrun:
        LOGGER.error("Script Aborted: Condor job not submitted")
        return

    # submit
    submit_result_obj = schedd_obj.submit(
        submit_obj,
        count=job_count,  # submit N jobs
        spool=True,  # for transfer_input_files
    )
    LOGGER.info(submit_result_obj)
    jobs = condor_tools.get_job_classads(
        submit_obj,
        job_count,
        submit_result_obj.cluster(),
    )
    schedd_obj.spool(jobs)

    # report to SkyDriver
    if skydriver_rc:
        update_skydriver(
            skydriver_rc,
            scan_id,
            submit_result_obj,
            collector,
            schedd,
        )
        LOGGER.info("Sent cluster info to SkyDriver")
