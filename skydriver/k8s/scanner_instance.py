"""Tools for creating K8s job objects for interacting with Skymap Scanner
instances."""

import logging
import textwrap
from pathlib import Path
from typing import Any

import yaml
from rest_tools.client import ClientCredentialsAuth

from .. import images
from ..config import (
    DebugMode,
    ENV,
)

LOGGER = logging.getLogger(__name__)

sdict = dict[str, Any]


class SkyScanK8sJobFactory:
    """Makes Skymap Scanner Kubernetes jobs, plus misc tools."""

    COMMON_SPACE_VOLUME_PATH = Path("/common-space")

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
        # s3 uploader
        s3_obj_url: str,
        # universal
        debug_mode: list[DebugMode],
        # env
        rest_address: str,
        skyscan_mq_client_timeout_wait_for_first_message: int | None,
        scanner_server_env_from_user: dict,
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
        scanner_server_envvars = SkyScanK8sJobFactory.make_skyscan_server_v1envvars(
            rest_address=rest_address,
            scan_id=scan_id,
            skyscan_mq_client_timeout_wait_for_first_message=skyscan_mq_client_timeout_wait_for_first_message,
            scanner_server_env_from_user=scanner_server_env_from_user,
        )

        # assemble the job
        job_dict = SkyScanK8sJobFactory._make_job(
            scan_id,
            docker_tag,
            s3_obj_url,
            scanner_server_memory_bytes,
            scanner_server_args,
            scanner_server_envvars,
        )

        return job_dict, scanner_server_args

    @staticmethod
    def _make_job(
        scan_id: str,
        docker_tag: str,
        s3_obj_url: str,
        scanner_server_memory_bytes: int,
        scanner_server_args: str,
        scanner_server_envvars: list[tuple[str, str]],
    ) -> sdict:
        """Create the K8s job manifest.

        NOTE: Let's keep definitions as straightforward as possible.
        """
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
                  containers:
                    - name: skyscan-server-{scan_id}
                      image: {images.get_skyscan_docker_image(docker_tag)}
                      command: []
                      args: {scanner_server_args.split()}
                      env:
                      resources:
                        limits:
                          memory: "{scanner_server_memory_bytes}"
                          cpu: "1"
                        requests:
                          memory: "{scanner_server_memory_bytes}"
                          cpu: "1"
                          ephemeral-storage: "1M"
                      volumeMounts:
                        - name: common-space-volume
                          mountPath: "{SkyScanK8sJobFactory.COMMON_SPACE_VOLUME_PATH}"
                    - name: s3-uploader-{scan_id}
                      restartPolicy: OnFailure
                      image: {images.get_skyscan_docker_image(docker_tag)}
                      command: []
                      args: ["echo", "{s3_obj_url}"]
                      resources:
                        limits:
                          memory: "256Mi"
                          cpu: "0.25"
                        requests:
                          memory: "256Mi"
                          cpu: "0.25"
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
            f" --client-startup-json {SkyScanK8sJobFactory.COMMON_SPACE_VOLUME_PATH/'startup.json'} "
            f" --nsides {' '.join(f'{n}:{x}' for n,x in nsides.items())} "  # k1:v1 k2:v2
            f" {'--real-event' if is_real_event else '--simulated-event'} "
            f" --predictive-scanning-threshold {predictive_scanning_threshold} "
        )
        return args

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
    def make_skyscan_server_v1envvars(
        rest_address: str,
        scan_id: str,
        skyscan_mq_client_timeout_wait_for_first_message: int | None,
        scanner_server_env_from_user: dict,
    ) -> list[tuple[str, str]]:
        """Get the environment variables provided to the skyscan server.

        Also, get the secrets' keys & their values.
        """
        LOGGER.debug(f"making scanner server env vars for {scan_id=}")
        env = []

        # 1. start w/ secrets
        # NOTE: the values come from an existing secret in the current namespace
        # *none*

        # 2. add required env vars
        required = {
            # broker/mq vars
            "SKYSCAN_BROKER_ADDRESS": ENV.SKYSCAN_BROKER_ADDRESS,
            # skydriver vars
            "SKYSCAN_SKYDRIVER_ADDRESS": rest_address,
            "SKYSCAN_SKYDRIVER_SCAN_ID": scan_id,
        }
        env.extend([(k, str(v)) for k, v in required.items()])

        # 3. add extra env vars, then filter out if 'None'
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
        env.extend([(k, str(v)) for k, v in prefiltered.items() if v is not None])

        # 4. generate & add auth tokens
        tokens = {
            "SKYSCAN_BROKER_AUTH": SkyScanK8sJobFactory._get_token_from_keycloak(
                ENV.KEYCLOAK_OIDC_URL,
                ENV.KEYCLOAK_CLIENT_ID_BROKER,
                ENV.KEYCLOAK_CLIENT_SECRET_BROKER,
            ),
            "SKYSCAN_SKYDRIVER_AUTH": SkyScanK8sJobFactory._get_token_from_keycloak(
                ENV.KEYCLOAK_OIDC_URL,
                ENV.KEYCLOAK_CLIENT_ID_SKYDRIVER_REST,
                ENV.KEYCLOAK_CLIENT_SECRET_SKYDRIVER_REST,
            ),
        }
        env.extend([(k, str(v)) for k, v in tokens.items()])

        # 5. Add user's env
        env.extend([(k, str(v)) for k, v in scanner_server_env_from_user.items()])

        return env
