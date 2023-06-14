"""For starting Skymap Scanner clients on an K8s cluster."""


import json
from pathlib import Path

import kubernetes  # type: ignore[import]

from ..config import LOGGER
from ..utils import S3File


def _get_log_fpath(logs_subdir: Path) -> Path:
    return logs_subdir / "clientmanager.log"


def make_k8s_job_desc(  # pylint: disable=too-many-argument
    # k8s args
    namespace: str,
    cluster_id: str,
    memory: str,
    n_jobs: int,
    n_cores: int,
    # skymap scanner args
    container_image: str,
    client_startup_json_s3: S3File,
    add_client_args: list[tuple[str, str]],
    # special args for the cloud
    cpu_arch: str = "x64",
) -> dict:
    """Make the k8s job description (submit object)."""
    if cpu_arch == "arm":
        with open("k8s_job_arm_stub.json", "r") as f:
            k8s_job_dict = json.load(f)
    else:
        with open("k8s_job_stub.json", "r") as f:
            k8s_job_dict = json.load(f)
    # Setting namespace
    k8s_job_dict["metadata"]["namespace"] = namespace
    k8s_job_dict["metadata"]["name"] = cluster_id

    # Setting parallelism
    k8s_job_dict["spec"]["completions"] = n_jobs
    k8s_job_dict["spec"]["parallelism"] = n_jobs

    # Setting JSON input file
    k8s_job_dict["spec"]["template"]["spec"]["initContainers"][0]["env"][0][
        "value"
    ] = client_startup_json_s3

    # Container image
    k8s_job_dict["spec"]["template"]["spec"]["containers"][0]["image"] = container_image

    # Adding more args to client
    client_args = k8s_job_dict["spec"]["template"]["spec"]["containers"][0]["args"]
    for carg, value in add_client_args:
        client_args.append(f"--{carg}")
        client_args.append(f"{value}")
    k8s_job_dict["spec"]["template"]["spec"]["containers"][0]["args"] = client_args

    return k8s_job_dict  # type: ignore[no-any-return]


def start(
    k8s_client: kubernetes.client.ApiClient,
    namespace: str,
    cluster_id: str,
    job_count: int,
    core_count: int,
    client_args: list[tuple[str, str]],
    memory: str,
    container_image: str,
    client_startup_json_s3: S3File,
    dryrun: bool,
    cpu_arch: str = "x64",
) -> dict:
    """Main logic."""

    # make k8s job description
    k8s_job_dict = make_k8s_job_desc(
        namespace,
        cluster_id,
        # condor args
        memory,
        job_count,
        core_count,
        # skymap scanner args
        container_image,
        client_startup_json_s3,
        client_args,
        cpu_arch,
    )

    # LOGGER.info(k8s_job_dict)

    # dryrun?
    if dryrun:
        LOGGER.info(k8s_job_dict)
        LOGGER.error("Script Aborted: K8s job not submitted")
        return k8s_job_dict

    # submit
    kubernetes.utils.create_from_dict(k8s_client, k8s_job_dict, namespace=namespace)

    return k8s_job_dict
