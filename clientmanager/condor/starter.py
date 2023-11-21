"""For starting Skymap Scanner clients on an HTCondor cluster."""


import datetime as dt
from pathlib import Path
from typing import Any

import htcondor  # type: ignore[import]

from ..config import ENV, FORWARDED_ENV_VARS, LOGGER
from ..utils import S3File


def make_condor_logs_subdir(directory: Path) -> Path:
    """Make the condor logs subdirectory."""
    iso_now = dt.datetime.now().isoformat(timespec="seconds")
    subdir = directory / f"skyscan-{iso_now}"
    subdir.mkdir(parents=True)
    LOGGER.info(f"HTCondor will write log files to {subdir}")
    return subdir


def make_condor_job_description(  # pylint: disable=too-many-arguments
    logs_subdir: Path | None,
    # condor args
    memory: str,
    n_cores: int,
    execution_time_limit: int,
    # skymap scanner args
    image: str,
    client_startup_json_s3: S3File,
    client_args_string: str,
) -> dict[str, Any]:
    """Make the condor job description (dict)."""

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
        "getenv": ", ".join(FORWARDED_ENV_VARS),
        "environment": f'"{environment}"',  # must be quoted
        "+FileSystemDomain": '"blah"',  # must be quoted
        #
        "should_transfer_files": "YES",
        "transfer_input_files": client_startup_json_s3.url,
        "transfer_output_files": '""',  # must be quoted for "none"
        #
        "request_cpus": str(n_cores),
        "request_memory": memory,
        "+WantIOProxy": "true",  # for HTChirp
        "+OriginalTime": execution_time_limit,  # Execution time limit -- 1 hour default on OSG
    }

    # outputs
    if logs_subdir:
        submit_dict.update(
            {
                "output": str(logs_subdir / "client-$(ProcId).out"),
                "error": str(logs_subdir / "client-$(ProcId).err"),
                "log": str(logs_subdir / "clientmanager.log"),
            }
        )
        # https://htcondor.readthedocs.io/en/latest/users-manual/file-transfer.html#specifying-if-and-when-to-transfer-files
        submit_dict.update(
            {
                "transfer_output_files": ",".join(
                    [
                        submit_dict["output"],  # type: ignore[list-item]
                        submit_dict["error"],  # type: ignore[list-item]
                        submit_dict["log"],  # type: ignore[list-item]
                    ]
                ),
                "when_to_transfer_output": "ON_EXIT_OR_EVICT",
            }
        )
    else:
        # NOTE: this needs to be removed if we ARE transferring files
        submit_dict["initialdir"] = "/tmp"

    return submit_dict


def prep(
    # starter CL args -- helper
    spool: bool,
    # starter CL args -- worker
    memory: str,
    n_cores: int,
    execution_time_limit: int,
    # starter CL args -- client
    client_args: list[tuple[str, str]],
    client_startup_json_s3: S3File,
    image: str,
) -> tuple[dict[str, Any], bool]:
    """Create objects needed for starting cluster."""
    if spool:
        logs_subdir = make_condor_logs_subdir()  # TODO- make path
    else:
        logs_subdir = None
        # NOTE: since we're not transferring any local files directly,
        # we don't need to spool. Files are on CVMFS and S3.

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
    submit_dict = make_condor_job_description(
        logs_subdir,
        # condor args
        memory,
        n_cores,
        execution_time_limit,
        # skymap scanner args
        image,
        client_startup_json_s3,
        client_args_string,
    )
    LOGGER.info(submit_dict)

    return submit_dict, spool


def start(
    schedd_obj: htcondor.Schedd,
    n_workers: int,
    #
    submit_dict: dict[str, Any],
    spool: bool,
) -> htcondor.SubmitResult:
    """Start cluster."""
    submit_obj = htcondor.Submit(submit_dict)
    LOGGER.info(submit_obj)

    # submit
    submit_result_obj = schedd_obj.submit(
        submit_obj,
        count=n_workers,  # submit N workers
        spool=spool,  # for transferring logs & files
    )
    LOGGER.info(submit_result_obj)
    if spool:
        jobs = list(
            submit_obj.jobs(
                count=n_workers,
                clusterid=submit_result_obj.cluster(),
            )
        )
        schedd_obj.spool(jobs)

    return submit_result_obj
