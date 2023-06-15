"""For starting Skymap Scanner clients on an HTCondor cluster."""


import datetime as dt
import getpass
from pathlib import Path

import htcondor  # type: ignore[import]

from ..config import ENV, LOGGER
from ..utils import S3File
from . import condor_tools


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
    logs_subdir: Path | None,
    # condor args
    memory: str,
    n_cores: int,
    accounting_group: str,
    # skymap scanner args
    image: str,
    client_startup_json_s3: S3File,
    client_args_string: str,
) -> htcondor.Submit:
    """Make the condor job description (submit object)."""

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
    env_vars = ["EWMS_PILOT_HTCHIRP=True"]
    # EWMS_* are inherited via condor `getenv`, but we have default in case these are not set.
    if not ENV.EWMS_PILOT_QUARANTINE_TIME:
        env_vars.append("EWMS_PILOT_QUARANTINE_TIME=1800")
    # The container sets I3_DATA to /opt/i3-data, however `millipede_wilks` requires files (spline tables) that are not available in the image. For the time being we require CVFMS and we load I3_DATA from there. In order to override the environment variables we need to prepend APPTAINERENV_ or SINGULARITYENV_ to the variable name. There are site-dependent behaviour but these two should cover all cases. See https://github.com/icecube/skymap_scanner/issues/135#issuecomment-1449063054.
    for prefix in ["APPTAINERENV_", "SINGULARITYENV_"]:
        env_vars.append(f"{prefix}I3_DATA=/cvmfs/icecube.opensciencegrid.org/data")
    environment = " ".join(env_vars)

    # write
    submit_dict = {
        "executable": "/bin/bash",
        "arguments": f"/usr/local/icetray/env-shell.sh python -m skymap_scanner.client {client_args_string} --client-startup-json ./{client_startup_json_s3.fname}",
        "+SingularityImage": f'"{image}"',  # must be quoted
        "Requirements": "HAS_CVMFS_icecube_opensciencegrid_org && has_avx && has_avx2",
        "getenv": "SKYSCAN_*, EWMS_*",
        "environment": f'"{environment}"',  # must be quoted
        "+FileSystemDomain": '"blah"',  # must be quoted
        "should_transfer_files": "YES",
        "transfer_input_files": client_startup_json_s3.url,
        "request_cpus": str(n_cores),
        "request_memory": memory,
        "notification": "Error",
        "+WantIOProxy": "true",  # for HTChirp
    }

    # outputs
    if logs_subdir:
        submit_dict.update(
            {
                "output": str(logs_subdir / "client-$(ProcId).out"),
                "error": str(logs_subdir / "client-$(ProcId).err"),
                "log": str(_get_log_fpath(logs_subdir)),
            }
        )
    else:
        # NOTE: this needs to be removed if we ARE transferring files
        submit_dict["initialdir"] = "/tmp"

    # accounting group
    if accounting_group:
        submit_dict["+AccountingGroup"] = f"{accounting_group}.{getpass.getuser()}"

    return htcondor.Submit(submit_dict)


def start(
    schedd_obj: htcondor.Schedd,
    n_workers: int,
    logs_directory: Path | None,
    client_args: list[tuple[str, str]],
    memory: str,
    n_cores: int,
    accounting_group: str,
    image: str,
    client_startup_json_s3: S3File,
    dryrun: bool,
) -> htcondor.SubmitResult:
    """Main logic."""
    if logs_directory:
        logs_subdir = make_condor_logs_subdir(logs_directory)
        spool = True
    else:
        logs_subdir = None
        # NOTE: since we're not transferring any local files directly,
        # we don't need to spool. Files are on CVMFS and S3.
        spool = False

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
        n_cores,
        accounting_group,
        # skymap scanner args
        image,
        client_startup_json_s3,
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
        count=n_workers,  # submit N workers
        spool=spool,  # for transferring logs & files
    )
    LOGGER.info(submit_result_obj)
    if spool:
        jobs = condor_tools.get_job_classads(
            submit_obj,
            n_workers,
            submit_result_obj.cluster(),
        )
        schedd_obj.spool(jobs)

    return submit_result_obj
