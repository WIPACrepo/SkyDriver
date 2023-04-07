"""For starting Skymap Scanner clients on an HTCondor cluster."""


import argparse
import dataclasses as dc
import datetime as dt
import getpass
import time
from pathlib import Path

import boto3  # type: ignore[import]
import htcondor  # type: ignore[import]
import requests
from wipac_dev_tools import argparse_tools

from .config import ENV, LOGGER

boto3.set_stream_logger(name="botocore")


@dc.dataclass
class S3File:
    """Wrap an S3 file."""

    url: str
    base_fname: str


def s3ify(filepath: Path) -> S3File:
    """Put the file in s3 and return info about it."""
    if not (
        ENV.EWMS_TMS_S3_URL
        and ENV.EWMS_TMS_S3_ACCESS_KEY
        and ENV.EWMS_TMS_S3_SECRET_KEY
        and ENV.EWMS_TMS_S3_BUCKET
        and ENV.SKYSCAN_SKYDRIVER_SCAN_ID
    ):
        raise RuntimeError(
            "must define all EWMS_TMS_S3_* environment variables to use S3"
        )
    s3_client = boto3.client(
        "s3",
        "us-east-1",
        endpoint_url=ENV.EWMS_TMS_S3_URL,
        aws_access_key_id=ENV.EWMS_TMS_S3_ACCESS_KEY,
        aws_secret_access_key=ENV.EWMS_TMS_S3_SECRET_KEY,
    )
    bucket = ENV.EWMS_TMS_S3_BUCKET
    key = ENV.SKYSCAN_SKYDRIVER_SCAN_ID

    # POST
    upload_details = s3_client.generate_presigned_post(bucket, key)
    with open(filepath, "rb") as f:
        response = requests.post(
            upload_details["url"],
            data=upload_details["fields"],
            files={"file": (filepath.name, f)},  # maps filename to obj
        )
    print(f"Upload response: {response.status_code}")
    print(str(response.content))

    # get GET url
    get_url = s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=3600,  # 60 mins
    )
    return S3File(get_url, filepath.name)


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
    accounting_group: str,
    # skymap scanner args
    singularity_image: str,
    client_startup_json_s3: S3File,
    client_args_string: str,
) -> htcondor.Submit:  # pylint:disable=no-member
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
    env_vars = []
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
        "arguments": f"/usr/local/icetray/env-shell.sh python -m skymap_scanner.client {client_args_string} --client-startup-json ./{client_startup_json_s3.base_fname}",
        "+SingularityImage": f'"{singularity_image}"',  # must be quoted
        "Requirements": "HAS_CVMFS_icecube_opensciencegrid_org && has_avx && has_avx2",
        "getenv": "SKYSCAN_*, EWMS_*",
        "environment": f'"{environment}"',  # must be quoted
        "+FileSystemDomain": '"blah"',  # must be quoted
        "should_transfer_files": "YES",
        "transfer_input_files": client_startup_json_s3.url,
        "request_cpus": "1",
        "request_memory": memory,
        "notification": "Error",
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

    # accounting group
    if accounting_group:
        submit_dict["+AccountingGroup"] = f"{accounting_group}.{getpass.getuser()}"

    return htcondor.Submit(submit_dict)  # pylint:disable=no-member


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
        default=None,
        type=Path,
        help="where to save logs (if not given, logs are not saved)",
    )

    # condor args
    sub_parser.add_argument(
        "--cluster",
        default=[None, None],  # list of a single 2-list
        nargs="*",
        type=lambda x: argparse_tools.validate_arg(
            (x.split(",")[0], x.split(",")[1], int(x.split(",")[2])),
            len(x.split(",")) == 3 and x.split(",")[2].isnumeric(),
            ValueError('must " "-delimited series of "collector,schedd,njobs"-tuples'),
        ),
        help=(
            "the HTCondor clusters to use, each entry contains: "
            "full DNS name of Collector server, full DNS name of Schedd server, # of jobs"
            "Ex: foo-bar.icecube.wisc.edu,baz.icecube.wisc.edu,123 alpha.icecube.wisc.edu,beta.icecube.wisc.edu,9999"
        ),
    )
    sub_parser.add_argument(
        "--accounting-group",
        default="",
        help=(
            "the accounting group to use, ex: 1_week. "
            "By default no accounting group is used."
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
    schedd_obj: htcondor.Schedd,  # pylint:disable=no-member
    job_count: int,
    logs_directory: Path | None,
    client_args: list[tuple[str, str]],
    memory: str,
    accounting_group: str,
    singularity_image: str,
    client_startup_json: Path,
    dryrun: bool,
) -> htcondor.SubmitResult:  # pylint:disable=no-member
    """Main logic."""
    if logs_directory:
        logs_subdir = make_condor_logs_subdir(logs_directory)
    else:
        logs_subdir = None

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

    # put client_startup_json in S3 bucket
    client_startup_json_s3 = s3ify(client_startup_json)

    # make condor job description
    submit_obj = make_condor_job_description(
        logs_subdir,
        # condor args
        memory,
        accounting_group,
        # skymap scanner args
        singularity_image,
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
        count=job_count,  # submit N jobs
    )
    LOGGER.info(submit_result_obj)
    # NOTE: since we're not transferring any local files directly,
    # we don't need to spool. Files are on CVMFS and S3.

    return submit_result_obj
