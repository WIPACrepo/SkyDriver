"""For starting Skymap Scanner clients on an HTCondor cluster."""


from pathlib import Path
from typing import Any

import htcondor  # type: ignore[import-untyped]
import humanfriendly

from ..config import ENV, FORWARDED_ENV_VARS, LOGGER
from ..utils import S3File


def make_condor_logs_dir() -> Path:
    """Make the condor logs subdirectory."""
    dpath = Path("tms-cluster")
    dpath.mkdir(parents=True)
    LOGGER.info(f"HTCondor will write log files to {dpath}")
    return dpath


def make_condor_job_description(
    spool: bool,
    # condor args
    worker_memory_bytes: int,
    worker_disk_bytes: int,
    n_cores: int,
    max_worker_runtime: int,
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
        # Don't transfer executable (/bin/bash) in case of
        #   version (dependency) mismatch.
        #     Ex:
        #     "/lib/x86_64-linux-gnu/libc.so.6: version `GLIBC_2.36' not found"
        # Technically this is just needed for spooling -- since if
        #   we don't spool, the executable (/bin/bash) can't be
        #   transferred anyway and so a local version will be used
        "transfer_executable": "false",
        #
        "request_cpus": str(n_cores),
        "request_memory": humanfriendly.format_size(worker_memory_bytes),  # -> "1 GB"
        "request_disk": humanfriendly.format_size(worker_disk_bytes),  # -> "1 GB"
        "+WantIOProxy": "true",  # for HTChirp
        "+OriginalTime": max_worker_runtime,  # Execution time limit -- 1 hour default on OSG
    }

    # outputs
    if spool:
        # this is the location where the files will go when/if *returned here*
        logs_dir = make_condor_logs_dir()
        submit_dict.update(
            {
                "output": str(logs_dir / "tms-worker-$(ProcId).out"),
                "error": str(logs_dir / "tms-worker-$(ProcId).err"),
                "log": str(logs_dir / "tms-cluster.log"),
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
    worker_memory_bytes: int,
    worker_disk_bytes: int,
    n_cores: int,
    max_worker_runtime: int,
    # starter CL args -- client
    client_args: list[tuple[str, str]],
    client_startup_json_s3: S3File,
    image: str,
) -> dict[str, Any]:
    """Create objects needed for starting cluster."""

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
        spool,
        # condor args
        worker_memory_bytes,
        worker_disk_bytes,
        n_cores,
        max_worker_runtime,
        # skymap scanner args
        image,
        client_startup_json_s3,
        client_args_string,
    )
    LOGGER.info(submit_dict)

    return submit_dict


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
