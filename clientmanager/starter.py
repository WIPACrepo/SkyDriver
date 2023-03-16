"""For starting Skymap Scanner clients on an HTCondor cluster."""


import argparse
import datetime as dt
import getpass
import os
import time
from pathlib import Path
from typing import Any

import htcondor  # type: ignore[import]
from rest_tools.client import RestClient

from . import condor_tools, utils
from .config import LOGGER

StrDict = dict[str, Any]


def make_condor_logs_subdir(directory: Path) -> Path:
    """Make the condor logs subdirectory."""
    iso_now = dt.datetime.now().isoformat(timespec="seconds")
    subdir = directory / f"skyscan-{iso_now}"
    subdir.mkdir(parents=True)
    LOGGER.info(f"HTCondor will write log files to {subdir}")
    return subdir


def _get_log_fpath(logs_subdir: Path) -> Path:
    return logs_subdir / "clientmanager.log"


def make_condor_submit_dict(  # pylint: disable=too-many-arguments
    logs_subdir: Path,
    # condor args
    memory: str,
    accounting_group: str,
    # skymap scanner args
    singularity_image: str,
    client_startup_json: Path,
    client_args: str,
) -> StrDict:
    """Make the condor submit dict."""
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
        "arguments": f"/usr/local/icetray/env-shell.sh python -m skymap_scanner.client {client_args} --client-startup-json ./{client_startup_json.name}",
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

    return submit_dict


def update_skydriver(
    skydriver_rc: RestClient,
    scan_id: str,
    submit_result: htcondor.SubmitResult,  # pylint:disable=no-member
    collector: str,
    schedd: str,
    submit_dict: StrDict,
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
                "submit_dict": submit_dict,
            }
        },
    )


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
        required=True,
        type=int,
        help="number of jobs",
        # default=4,
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
        nargs="+",
        help="n 'key:value' pairs containing the python CL arguments to pass to skymap_scanner.client",
    )


def start(args: argparse.Namespace) -> None:
    """Main logic."""
    logs_subdir = make_condor_logs_subdir(args.logs_directory)

    # get client args
    client_args = ""
    if args.client_args is not None:
        for carg_value in args.client_args:
            carg, value = carg_value.split(":", maxsplit=1)
            client_args += f" --{carg} {value} "
        LOGGER.info(f"Client Args: {client_args}")
        if "--client-startup-json" in client_args:
            raise RuntimeError(
                "The '--client-args' arg cannot include \"--client-startup-json\". "
                "This needs to be given to this script explicitly ('--client-startup-json')."
            )

    # write condor token file (before any condor calls)
    if token := os.getenv("CONDOR_TOKEN"):
        condor_tokens_dpath = Path("~/.condor/tokens.d/").expanduser()
        condor_tokens_dpath.mkdir(parents=True, exist_ok=True)
        with open(condor_tokens_dpath / "token1", "w") as f:
            f.write(token)

    # make condor job description
    submit_dict = make_condor_submit_dict(
        logs_subdir,
        # condor args
        args.memory,
        args.accounting_group,
        # skymap scanner args
        args.singularity_image,
        args.client_startup_json,
        client_args,
    )
    submit_obj = htcondor.Submit(submit_dict)  # pylint:disable=no-member
    LOGGER.info(submit_obj)

    # dryrun?
    if args.dryrun:
        LOGGER.error("Script Aborted: Condor job not submitted")
        return

    # make connections -- do now so we don't have any surprises
    skydriver_rc, scan_id = utils.connect_to_skydriver()
    schedd_obj = condor_tools.get_schedd_obj(args.collector, args.schedd)

    # submit
    submit_result_obj = schedd_obj.submit(
        submit_obj,
        count=args.jobs,  # submit N jobs
        spool=True,  # for transfer_input_files
    )
    LOGGER.info(submit_result_obj)
    jobs = condor_tools.get_job_classads(
        submit_obj, args.jobs, submit_result_obj.cluster()
    )
    schedd_obj.spool(jobs)

    # report to SkyDriver
    if skydriver_rc:
        update_skydriver(
            skydriver_rc,
            scan_id,
            submit_result_obj,
            args.collector,
            args.schedd,
            submit_dict,
        )
        LOGGER.info("Sent cluster info to SkyDriver")
