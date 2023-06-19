"""For starting Skymap Scanner clients on an K8s cluster."""


import json
from pathlib import Path

import kubernetes  # type: ignore[import]

from ..config import ENV, LOGGER
from ..utils import S3File


def _get_log_fpath(logs_subdir: Path) -> Path:
    return logs_subdir / "clientmanager.log"


def make_k8s_job_desc(
    job_config_stub: Path,
    # k8s args
    host: str,
    namespace: str,
    cluster_id: str,
    memory: str,
    n_workers: int,
    n_cores: int,
    # skymap scanner args
    container_image: str,
    client_startup_json_s3: S3File,
    add_client_args: list[tuple[str, str]],
    # special args for the cloud
    cpu_arch: str,
) -> dict:
    """Make the k8s job description (submit object)."""
    with open(job_config_stub, "r") as f:
        k8s_job_dict = json.load(f)

    # multiple different variations add to these...
    for meta_field in ["labels", "annotations"]:
        if meta_field not in k8s_job_dict["metadata"]:
            k8s_job_dict["metadata"][meta_field] = {}

    # ARM-specific fields
    if cpu_arch == "arm":
        # labels
        k8s_job_dict["metadata"]["labels"].update({"beta.kubernetes.io/arch": "arm64"})
        # affinity
        k8s_job_dict["spec"]["template"]["spec"]["affinity"] = {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": "kubernetes.io/arch",
                                    "operator": "In",
                                    "values": ["arm64"],
                                }
                            ]
                        }
                    ]
                }
            }
        }

    # Setting namespace
    k8s_job_dict["metadata"]["namespace"] = namespace
    k8s_job_dict["metadata"]["name"] = cluster_id

    if host == "local":
        k8s_job_dict["metadata"]["labels"].update(
            {
                # https://argo-cd.readthedocs.io/en/stable/user-guide/resource_tracking/
                "app.kubernetes.io/instance": ENV.WORKER_K8S_LOCAL_APPLICATION_NAME,
            }
        )
        k8s_job_dict["metadata"]["annotations"].update(
            {
                "argocd.argoproj.io/sync-options": "Prune=false"  # don't want argocd to prune this job
            }
        )

    # Setting parallelism
    k8s_job_dict["spec"]["completions"] = n_workers
    k8s_job_dict["spec"]["parallelism"] = n_workers

    # set memory & # cores
    k8s_job_dict["spec"]["template"]["spec"]["containers"][0]["resources"] = {
        "limits": {
            "cpu": str(n_cores),
            "memory": memory,  # TODO: give a bit more just in case?
        },
        "requests": {
            "cpu": str(n_cores),
            "memory": memory,
        },
    }

    # Setting JSON input file url
    k8s_job_dict["spec"]["template"]["spec"]["initContainers"][0]["env"][0][
        "value"
    ] = client_startup_json_s3.url

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
    job_config_stub: Path,
    host: str,
    namespace: str,
    cluster_id: str,
    n_workers: int,
    n_cores: int,
    client_args: list[tuple[str, str]],
    memory: str,
    container_image: str,
    client_startup_json_s3: S3File,
    dryrun: bool,
    cpu_arch: str,
) -> dict:
    """Main logic."""
    if host == "local" and n_workers > ENV.WORKER_K8S_MAX_LOCAL_WORKERS:
        LOGGER.warning(
            f"Requested more workers ({n_workers}) than the max allowed {ENV.WORKER_K8S_MAX_LOCAL_WORKERS}. Using the maximum instead."
        )
        n_workers = ENV.WORKER_K8S_MAX_LOCAL_WORKERS

    # make k8s job description
    k8s_job_dict = make_k8s_job_desc(
        job_config_stub,
        host,
        namespace,
        cluster_id,
        # condor args
        memory,
        n_workers,
        n_cores,
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
