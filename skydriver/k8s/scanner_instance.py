"""Tools for creating K8s job objects for interacting with Skymap Scanner
instances."""

import logging
import textwrap
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import aiocache  # type: ignore[import-untyped]
import yaml
from dateutil import parser
from rest_tools.client import ClientCredentialsAuth, RestClient

from .. import ewms, images
from ..config import (
    DebugMode,
    ENV,
    SCANNER_LOGS_GRAFANA_WINDOW_SEC,
    SCANNER_LOGS_PROMETHEUS_SEARCH_WINDOW_HRS,
    sdict,
)
from ..database.schema import Manifest
from ..images import get_skyscan_cvmfs_apptainer_image_path

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
        scanner_server_env_from_user: dict,
        request_clusters: list,
        max_pixel_reco_time: int,
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
            scan_id=scan_id,
            scanner_server_env_from_user=scanner_server_env_from_user,
        )

        ewms_envvars = EnvVarFactory.make_ewms_envvars(
            docker_tag,
            #
            request_clusters,
            #
            max_pixel_reco_time,
            debug_mode,
            #
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
                "value": str(ENV.K8S_SCANNER_SIDECAR_S3_LIFETIME_SECONDS),
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
                          ephemeral-storage: "8G"
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
        max_pixel_reco_time: int,
        debug_mode: list[DebugMode],
        #
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
                "EWMS_TASK_IMAGE": str(
                    get_skyscan_cvmfs_apptainer_image_path(docker_tag)
                ),
                #
                "EWMS_PILOT_TIMEOUT_QUEUE_INCOMING": ENV.EWMS_PILOT_TIMEOUT_QUEUE_INCOMING,
                "EWMS_PILOT_TASK_TIMEOUT": max_pixel_reco_time,
                "EWMS_PILOT_QUARANTINE_TIME": max_pixel_reco_time,  # piggy-back
                "EWMS_PILOT_DUMP_TASK_OUTPUT": bool(
                    DebugMode.CLIENT_LOGS in debug_mode
                ),
                #
                "EWMS_WORKER_MAX_WORKER_RUNTIME": ENV.EWMS_MAX_WORKER_RUNTIME__DEFAULT,
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
            {"name": "S3_EXPIRES_IN", "value": str(ENV.S3_EXPIRES_IN)},
            {"name": "S3_BUCKET", "value": ENV.S3_BUCKET},
            {"name": "S3_OBJECT_KEY", "value": ewms.make_s3_object_key(scan_id)},
        ]

    @staticmethod
    def get_skydriver_rest_auth() -> str:
        if ENV.CI:
            return ""
        cca = ClientCredentialsAuth(
            "",
            token_url=ENV.KEYCLOAK_OIDC_URL,
            client_id=ENV.KEYCLOAK_CLIENT_ID_SKYDRIVER_REST,
            client_secret=ENV.KEYCLOAK_CLIENT_SECRET_SKYDRIVER_REST,
        )
        token = cca.make_access_token()
        return token

    @staticmethod
    def make_skyscan_server_envvars(
        scan_id: str,
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
            "SKYSCAN_SKYDRIVER_ADDRESS": ENV.HERE_URL,
            "SKYSCAN_SKYDRIVER_SCAN_ID": scan_id,
        }
        env.update(required)

        # 2. add extra env vars, then filter out if 'None'
        prefiltered = {
            "SKYSCAN_PROGRESS_INTERVAL_SEC": ENV.SKYSCAN_PROGRESS_INTERVAL_SEC,
            "SKYSCAN_RESULT_INTERVAL_SEC": ENV.SKYSCAN_RESULT_INTERVAL_SEC,
            #
            "SKYSCAN_MQ_TIMEOUT_FROM_CLIENTS": ENV.SKYSCAN_MQ_TIMEOUT_FROM_CLIENTS,
            #
            "SKYSCAN_LOG": ENV.SKYSCAN_LOG,
            "SKYSCAN_LOG_THIRD_PARTY": ENV.SKYSCAN_LOG_THIRD_PARTY,
            #
            "SKYSCAN_EWMS_PILOT_LOG": "WARNING",  # default is too low
            "SKYSCAN_MQ_CLIENT_LOG": "WARNING",  # default is too low
        }
        env.update({k: str(v) for k, v in prefiltered.items() if v is not None})

        # 3. generate & add auth tokens
        tokens = {
            "SKYSCAN_SKYDRIVER_AUTH": EnvVarFactory.get_skydriver_rest_auth(),
        }
        env.update(tokens)

        # 4. Add user's env
        env.update(scanner_server_env_from_user)

        return [{"name": str(k), "value": str(v)} for k, v in env.items()]


class LogWrangler:
    """Tools for retrieving logs."""

    @staticmethod
    def get_scan_start_time(manifest: Manifest) -> int:
        """Get the timestamp for when the scan started."""
        if manifest.progress:  # the scan has started
            if ret := manifest.progress.start:
                return ret
            # 'ret==None' for a started scan probably indicates the scan is pre-v2
            try:
                dt = parser.parse(
                    manifest.progress.processing_stats.start["scanner start"]
                )
                return int(dt.timestamp())
            except Exception as e:
                LOGGER.error(f"could not parse scan start time: {e}")
                pass

        # fall-through
        return int(manifest.timestamp)  # when the scan was requested

    @staticmethod
    def _get_grafana_logs_url(scan_id: str, end_timestamp: int | None) -> str:
        url = (
            f"{ENV.GRAFANA_DASHBOARD_BASEURL}"
            f"&var-namespace={ENV.K8S_NAMESPACE}"
            f"&var-container={get_skyscan_server_container_name(scan_id)}"
        )
        if end_timestamp:
            url += (
                f"&from={(end_timestamp-SCANNER_LOGS_GRAFANA_WINDOW_SEC)*1000}"
                f"&to={end_timestamp*1000}"
            )
        return url

    @staticmethod
    @aiocache.cached(ttl=ENV.CACHE_DURATION_PROMETHEUS)  # fyi: logs may/will be updated
    async def _query_prometheus_for_timerange(
        scan_id: str, search_start_ts: int
    ) -> int | None:
        search_end_ts = search_start_ts + (
            SCANNER_LOGS_PROMETHEUS_SEARCH_WINDOW_HRS * 60 * 60
        )
        if time.time() < search_end_ts:
            # this is a recent scan, no need to query
            return None
        if ENV.CI:  # for ci testing
            return None

        params = {
            "query": (
                "last_over_time(timestamp(changes(kube_pod_container_status_running{container="
                f'"{get_skyscan_server_container_name(scan_id)}"'
                "}[1m])>=0)["
                f"{str(SCANNER_LOGS_PROMETHEUS_SEARCH_WINDOW_HRS)}"
                "h:])"
            ),
            "start": str(search_start_ts),
            "end": str(search_end_ts),
            "step": "1m",
        }

        # query prometheus for timerange
        url = f"{ENV.PROMETHEUS_URL}?{urlencode(params)}"
        try:
            rc = RestClient(
                url,
                username=ENV.PROMETHEUS_AUTH_USERNAME,
                password=ENV.PROMETHEUS_AUTH_PASSWORD,
            )
            response = await rc.request("GET", "")
            return int(response.json()["data"]["result"][0]["values"][-1][-1])
        except Exception as e:  # this is all or nothing, so except and move on
            LOGGER.error(
                f"could not get end time for scanner logs using prometheus query ({url}): {e}"
            )
            return None

    @staticmethod
    async def assemble_scanner_server_logs_url(manifest: Manifest) -> str:
        """Get the URL pointing to a web dashboard for viewing the scanner server's logs."""
        start = LogWrangler.get_scan_start_time(manifest)
        logs_end_ts = await LogWrangler._query_prometheus_for_timerange(
            manifest.scan_id, start
        )
        return LogWrangler._get_grafana_logs_url(manifest.scan_id, logs_end_ts)
