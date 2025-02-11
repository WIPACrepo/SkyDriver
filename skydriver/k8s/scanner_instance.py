"""Tools for creating K8s job objects for interacting with Skymap Scanner
instances."""

import logging
import textwrap
from pathlib import Path
from typing import Any

import kubernetes.client  # type: ignore[import-untyped]
import yaml
from rest_tools.client import ClientCredentialsAuth

from .utils import KubeAPITools
from .. import ewms, images
from ..config import (
    DebugMode,
    ENV,
    sdict,
)
from ..images import get_skyscan_cvmfs_singularity_image

LOGGER = logging.getLogger(__name__)


def get_skyscan_server_container_name(scan_id: str) -> str:
    """Get the k8s container name for the scanner server from the scan_id (deterministic)."""
    return f"skyscan-server-{scan_id}"


def _to_inline_yaml_str(obj: Any) -> str:
    """Convert obj to one-line yaml-syntax."""
    return yaml.safe_dump(
        obj,
        default_flow_style=True,  # inline, compact formatting, no indenting needed
    )


class SkyScanK8sJobFactory:
    """Makes Skymap Scanner Kubernetes jobs, plus misc tools."""

    COMMON_SPACE_VOLUME_PATH = Path("/common-space")
    _STARTUP_JSON_FPATH = COMMON_SPACE_VOLUME_PATH / "startup.json"
    _EWMS_JSON_FPATH = COMMON_SPACE_VOLUME_PATH / "ewms.json"

    @staticmethod
    def make(
        #
        docker_tag: str,
        scan_id: str,
        # scanner
        scanner_server_memory_bytes: int,
        reco_algo: str,
        nsides: dict[int, int],
        is_real_event: bool,
        predictive_scanning_threshold: float,
        # universal
        debug_mode: list[DebugMode],
        # env
        rest_address: str,
        skyscan_mq_client_timeout_wait_for_first_message: int | None,
        scanner_server_env_from_user: dict,
        request_clusters: list,
        max_pixel_reco_time: int,
        max_worker_runtime: int,
        priority: int,
        worker_disk_bytes: int,
        worker_memory_bytes: int,
    ) -> tuple[sdict, str]:
        """Make the K8s job dict.

        Also, returns the server's args (so the user can see this later).
        """
        LOGGER.info(f"making k8s job for {scan_id=}")

        # pre-create some job components
        scanner_server_args = SkyScanK8sJobFactory.get_scanner_server_args(
            reco_algo=reco_algo,
            nsides=nsides,
            is_real_event=is_real_event,
            predictive_scanning_threshold=predictive_scanning_threshold,
        )
        scanner_server_envvars = EnvVarFactory.make_skyscan_server_envvars(
            rest_address=rest_address,
            scan_id=scan_id,
            skyscan_mq_client_timeout_wait_for_first_message=skyscan_mq_client_timeout_wait_for_first_message,
            scanner_server_env_from_user=scanner_server_env_from_user,
        )

        ewms_envvars = EnvVarFactory.make_ewms_envvars(
            docker_tag,
            #
            request_clusters,
            #
            skyscan_mq_client_timeout_wait_for_first_message,
            max_pixel_reco_time,
            #
            max_worker_runtime,
            priority,
            worker_disk_bytes,
            worker_memory_bytes,
        )

        # assemble the job
        job_dict = SkyScanK8sJobFactory._make_job(
            scan_id,
            docker_tag,
            scanner_server_memory_bytes,
            scanner_server_args,
            scanner_server_envvars,
            ewms_envvars,
        )

        return job_dict, scanner_server_args

    @staticmethod
    def _make_job(
        scan_id: str,
        docker_tag: str,
        scanner_server_memory_bytes: int,
        scanner_server_args: str,
        scanner_server_envvars: list[sdict],
        ewms_envvars: list[sdict],
    ) -> sdict:
        """Create the K8s job manifest.

        NOTE: Let's keep definitions as straightforward as possible.
        """
        ewms_init_envvars = (
            [
                envvar
                for envvar in scanner_server_envvars
                if envvar["name"]
                in ["SKYSCAN_SKYDRIVER_ADDRESS", "SKYSCAN_SKYDRIVER_AUTH"]
            ]
            + ewms_envvars
            + EnvVarFactory.make_s3_envvars(scan_id)
        )
        s3_sidecar_envvars = [
            {
                "name": "K8S_SCANNER_SIDECAR_S3_LIFETIME_SECONDS",
                "value": ENV.K8S_SCANNER_SIDECAR_S3_LIFETIME_SECONDS,
            }
        ] + EnvVarFactory.make_s3_envvars(scan_id)

        # now, assemble
        job_yaml = textwrap.dedent(  # fixes """-indentation
            f"""
            apiVersion: batch/v1
            kind: Job
            metadata:
              namespace: {ENV.K8S_NAMESPACE}
              name: {SkyScanK8sJobFactory.get_job_name(scan_id)}
              labels:
                app.kubernetes.io/instance: {ENV.K8S_APPLICATION_NAME}
              annotations:
                argocd.argoproj.io/sync-options: "Prune=false"
            spec:
              ttlSecondsAfterFinished: {ENV.K8S_TTL_SECONDS_AFTER_FINISHED}
              backoffLimit: 0
              activeDeadlineSeconds: {ENV.K8S_ACTIVE_DEADLINE_SECONDS}
              template:
                metadata:
                  labels:
                    app: scanner-instance
                spec:
                  serviceAccountName: {ENV.K8S_SKYSCAN_JOBS_SERVICE_ACCOUNT}
                  restartPolicy: Never
                  initContainers:
                    - name: init-ewms-{scan_id}
                      image: {ENV.THIS_IMAGE_WITH_TAG}
                      command: ["python", "-m", "ewms_init_container"]
                      args: ["{scan_id}", "--json-out", "{SkyScanK8sJobFactory._EWMS_JSON_FPATH}"]
                      env: {_to_inline_yaml_str(ewms_init_envvars)}
                      resources:
                        limits:
                          memory: "{ENV.K8S_SCANNER_INIT_MEM_LIMIT}"
                          cpu: "{ENV.K8S_SCANNER_INIT_CPU_LIMIT}"
                        requests:
                          memory: "{ENV.K8S_SCANNER_INIT_MEM_REQUEST}"
                          cpu: "{ENV.K8S_SCANNER_INIT_CPU_REQUEST}"
                          ephemeral-storage: "1M"
                      volumeMounts:
                        - name: common-space-volume
                          mountPath: "{SkyScanK8sJobFactory.COMMON_SPACE_VOLUME_PATH}"
                  containers:
                    - name: {get_skyscan_server_container_name(scan_id)}
                      image: {images.get_skyscan_docker_image(docker_tag)}
                      command: []
                      args: {_to_inline_yaml_str(scanner_server_args.split())}
                      env: {_to_inline_yaml_str(scanner_server_envvars)}
                      resources:
                        limits:
                          memory: "{scanner_server_memory_bytes}"
                          cpu: "{ENV.K8S_SCANNER_CPU_LIMIT}"
                        requests:
                          memory: "{scanner_server_memory_bytes}"
                          cpu: "{ENV.K8S_SCANNER_CPU_REQUEST}"
                          ephemeral-storage: "1M"
                      volumeMounts:
                        - name: common-space-volume
                          mountPath: "{SkyScanK8sJobFactory.COMMON_SPACE_VOLUME_PATH}"
                    - name: sidecar-s3-{scan_id}
                      restartPolicy: OnFailure
                      image: {ENV.THIS_IMAGE_WITH_TAG}
                      command: ["python", "-m", "s3_sidecar"]
                      args: ["{SkyScanK8sJobFactory._STARTUP_JSON_FPATH}", "--wait-indefinitely"]
                      env: {_to_inline_yaml_str(s3_sidecar_envvars)}
                      resources:
                        limits:
                          memory: "{ENV.K8S_SCANNER_SIDECAR_S3_MEM_LIMIT}"
                          cpu: "{ENV.K8S_SCANNER_SIDECAR_S3_CPU_LIMIT}"
                        requests:
                          memory: "{ENV.K8S_SCANNER_SIDECAR_S3_MEM_REQUEST}"
                          cpu: "{ENV.K8S_SCANNER_SIDECAR_S3_CPU_REQUEST}"
                          ephemeral-storage: "1M"
                      volumeMounts:
                        - name: common-space-volume
                          mountPath: "{SkyScanK8sJobFactory.COMMON_SPACE_VOLUME_PATH}"
                  volumes:
                    - name: common-space-volume
                      emptyDir: {{}}
            """
        )

        # Parse the YAML string into a Python dictionary
        job_dict = yaml.safe_load(job_yaml)
        return job_dict

    @staticmethod
    def get_job_name(scan_id: str) -> str:
        """Get the name of the K8s job (deterministic)."""
        return f"skyscan-{scan_id}"

    @staticmethod
    def get_scanner_server_args(
        reco_algo: str,
        nsides: dict[int, int],
        is_real_event: bool,
        predictive_scanning_threshold: float,
    ) -> str:
        """Make the server container args."""
        args = (
            f"python -m skymap_scanner.server "
            f" --reco-algo {reco_algo}"
            f" --cache-dir {SkyScanK8sJobFactory.COMMON_SPACE_VOLUME_PATH} "
            # f" --output-dir {common_space_volume_path} "  # output is sent to skydriver
            f" --client-startup-json {SkyScanK8sJobFactory._STARTUP_JSON_FPATH} "
            f" --nsides {' '.join(f'{n}:{x}' for n,x in nsides.items())} "  # k1:v1 k2:v2
            f" {'--real-event' if is_real_event else '--simulated-event'} "
            f" --predictive-scanning-threshold {predictive_scanning_threshold} "
        )
        return args


class EnvVarFactory:
    """Factory class for assembling k8s environment-variable objects."""

    @staticmethod
    def make_ewms_envvars(
        docker_tag: str,
        #
        request_clusters: list,
        #
        skyscan_mq_client_timeout_wait_for_first_message: int | None,
        max_pixel_reco_time: int,
        #
        max_worker_runtime: int,
        priority: int,
        worker_disk_bytes: int,
        worker_memory_bytes: int,
    ) -> list[sdict]:
        return [
            {"name": str(k), "value": str(v)}
            for k, v in {
                "EWMS_ADDRESS": ENV.EWMS_ADDRESS,
                "EWMS_TOKEN_URL": ENV.EWMS_TOKEN_URL,
                "EWMS_CLIENT_ID": ENV.EWMS_CLIENT_ID,
                "EWMS_CLIENT_SECRET": ENV.EWMS_CLIENT_SECRET,
                #
                "EWMS_CLUSTERS": " ".join(cname for cname, _ in request_clusters),
                "EWMS_N_WORKERS": request_clusters[0][1],
                #
                "EWMS_TASK_IMAGE": get_skyscan_cvmfs_singularity_image(docker_tag),
                #
                "EWMS_PILOT_TIMEOUT_QUEUE_WAIT_FOR_FIRST_MESSAGE": skyscan_mq_client_timeout_wait_for_first_message,
                "EWMS_PILOT_TIMEOUT_QUEUE_INCOMING": ENV.SKYSCAN_MQ_TIMEOUT_TO_CLIENTS,
                "EWMS_PILOT_TASK_TIMEOUT": max_pixel_reco_time,
                #
                "EWMS_WORKER_MAX_WORKER_RUNTIME": max_worker_runtime,
                "EWMS_WORKER_PRIORITY": priority,
                "EWMS_WORKER_DISK_BYTES": worker_disk_bytes,
                "EWMS_WORKER_MEMORY_BYTES": worker_memory_bytes,
            }.items()
            if v is not None
        ]

    @staticmethod
    def make_s3_envvars(scan_id: str) -> list[sdict]:
        return [
            {"name": "S3_URL", "value": ENV.S3_URL},
            {
                "name": "S3_ACCESS_KEY_ID",
                "valueFrom": {
                    "secretKeyRef": {
                        "name": ENV.K8S_SECRET_NAME,
                        "key": ENV.S3_ACCESS_KEY_ID__K8S_SECRET_KEY,
                    }
                },
            },
            {
                "name": "S3_SECRET_KEY",
                "valueFrom": {
                    "secretKeyRef": {
                        "name": ENV.K8S_SECRET_NAME,
                        "key": ENV.S3_SECRET_KEY__K8S_SECRET_KEY,
                    }
                },
            },
            {"name": "S3_EXPIRES_IN", "value": ENV.S3_EXPIRES_IN},
            {"name": "S3_BUCKET", "value": ENV.S3_BUCKET},
            {"name": "S3_OBJECT_KEY", "value": ewms.make_s3_object_key(scan_id)},
            {
                "name": "K8S_SCANNER_SIDECAR_S3_LIFETIME_SECONDS",
                "value": ENV.K8S_SCANNER_SIDECAR_S3_LIFETIME_SECONDS,
            },
        ]

    @staticmethod
    def _get_token_from_keycloak(
        token_url: str,
        client_id: str,
        client_secret: str,
    ) -> str:
        if not token_url:  # would only be falsy in test
            return ""
        cca = ClientCredentialsAuth(
            "",
            token_url=token_url,
            client_id=client_id,
            client_secret=client_secret,
        )
        token = cca.make_access_token()
        return token

    @staticmethod
    def make_skyscan_server_envvars(
        rest_address: str,
        scan_id: str,
        skyscan_mq_client_timeout_wait_for_first_message: int | None,
        scanner_server_env_from_user: dict,
    ) -> list[sdict]:
        """Get the environment variables provided to the skyscan server."""
        LOGGER.debug(f"making scanner server env vars for {scan_id=}")
        env = {}

        # 1. add required env vars
        required = {
            # broker/mq vars
            "SKYSCAN_EWMS_JSON": str(SkyScanK8sJobFactory._EWMS_JSON_FPATH),
            # skydriver vars
            "SKYSCAN_SKYDRIVER_ADDRESS": rest_address,
            "SKYSCAN_SKYDRIVER_SCAN_ID": scan_id,
        }
        env.update(required)

        # 2. add extra env vars, then filter out if 'None'
        prefiltered = {
            "SKYSCAN_PROGRESS_INTERVAL_SEC": ENV.SKYSCAN_PROGRESS_INTERVAL_SEC,
            "SKYSCAN_RESULT_INTERVAL_SEC": ENV.SKYSCAN_RESULT_INTERVAL_SEC,
            #
            "SKYSCAN_MQ_TIMEOUT_TO_CLIENTS": ENV.SKYSCAN_MQ_TIMEOUT_TO_CLIENTS,
            "SKYSCAN_MQ_TIMEOUT_FROM_CLIENTS": ENV.SKYSCAN_MQ_TIMEOUT_FROM_CLIENTS,
            #
            "SKYSCAN_LOG": ENV.SKYSCAN_LOG,
            "SKYSCAN_LOG_THIRD_PARTY": ENV.SKYSCAN_LOG_THIRD_PARTY,
            #
            "SKYSCAN_EWMS_PILOT_LOG": "WARNING",  # default is too low
            "SKYSCAN_MQ_CLIENT_LOG": "WARNING",  # default is too low
            #
            "SKYSCAN_MQ_CLIENT_TIMEOUT_WAIT_FOR_FIRST_MESSAGE": skyscan_mq_client_timeout_wait_for_first_message,
        }
        env.update({k: str(v) for k, v in prefiltered.items() if v is not None})

        # 3. generate & add auth tokens
        tokens = {
            "SKYSCAN_SKYDRIVER_AUTH": EnvVarFactory._get_token_from_keycloak(
                ENV.KEYCLOAK_OIDC_URL,
                ENV.KEYCLOAK_CLIENT_ID_SKYDRIVER_REST,
                ENV.KEYCLOAK_CLIENT_SECRET_SKYDRIVER_REST,
            ),
        }
        env.update(tokens)

        # 4. Add user's env
        env.update(scanner_server_env_from_user)

        return [{"name": str(k), "value": str(v)} for k, v in env.items()]


def assemble_scanner_server_logs_url(
    k8s_batch_api: kubernetes.client.BatchV1Api,
    scan_id: str,
) -> str:
    """Get the URL pointing to a web dashboard for viewing the scanner server's logs."""
    job_name = SkyScanK8sJobFactory.get_job_name(scan_id)
    k8s_core_api = kubernetes.client.CoreV1Api(api_client=k8s_batch_api.api_client)

    try:
        for podname in KubeAPITools.get_pods(k8s_core_api, job_name, ENV.K8S_NAMESPACE):
            # this is an iterator, but in reality, the job should only map to 1 pod
            return (
                f"{ENV.GRAFANA_DASHBOARD_BASEURL}"
                f"&var-namespace={ENV.K8S_NAMESPACE}"
                f"&var-pod={podname}"
                f"&var-container={get_skyscan_server_container_name(scan_id)}"
            )
    except Exception as e:
        LOGGER.error(f"there was an issue retrieving k8s pod(s) for {scan_id=}")
        LOGGER.exception(e)

    # fall-through
    return "404"  # don't return exception info for security reasons
