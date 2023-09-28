"""Tools for creating K8s job objects for interacting with Skymap Scanner
instances."""


import dataclasses as dc
from pathlib import Path
from typing import Any

import kubernetes.client  # type: ignore[import]
from rest_tools.client import ClientCredentialsAuth

from .. import database, images
from ..config import ENV, KNOWN_CLUSTERS, LOGGER, DebugMode
from ..database import schema
from . import scan_backlog
from .utils import KubeAPITools


def get_cluster_auth_v1envvars(
    cluster: schema.Cluster,
) -> list[kubernetes.client.V1EnvVar]:
    """Get the `V1EnvVar`s for workers' auth."""
    LOGGER.debug(f"getting auth secret env vars for {cluster=}")
    info = next(
        x
        for x in KNOWN_CLUSTERS.values()
        if x["location"] == dc.asdict(cluster.location)
    )
    return info["v1envvars"]  # type: ignore[return-value]


def get_tms_s3_v1envvars() -> list[kubernetes.client.V1EnvVar]:
    """Get the `V1EnvVar`s for TMS's S3 auth."""
    return [
        kubernetes.client.V1EnvVar(
            name="EWMS_TMS_S3_ACCESS_KEY_ID",
            value_from=kubernetes.client.V1EnvVarSource(
                secret_key_ref=kubernetes.client.V1SecretKeySelector(
                    name=ENV.K8S_SECRET_NAME,
                    key="ewms_tms_s3_access_key_id",
                )
            ),
        ),
        kubernetes.client.V1EnvVar(
            name="EWMS_TMS_S3_SECRET_KEY",
            value_from=kubernetes.client.V1EnvVarSource(
                secret_key_ref=kubernetes.client.V1SecretKeySelector(
                    name=ENV.K8S_SECRET_NAME,
                    key="ewms_tms_s3_secret_key",
                )
            ),
        ),
    ]


class SkymapScannerJob:
    """Wraps a Skymap Scanner Kubernetes job with tools to start and manage."""

    def __init__(
        self,
        k8s_batch_api: kubernetes.client.BatchV1Api,
        scan_backlog: database.interface.ScanBacklogClient,
        #
        docker_tag: str,
        scan_id: str,
        # scanner
        scanner_server_memory: str,
        reco_algo: str,
        nsides: dict[int, int],
        is_real_event: bool,
        predictive_scanning_threshold: float,
        # tms
        memory: str,
        request_clusters: list[schema.Cluster],
        max_pixel_reco_time: int,
        # universal
        debug_mode: list[DebugMode],
        # env
        rest_address: str,
    ):
        LOGGER.info(f"making k8s job for {scan_id=}")
        self.k8s_batch_api = k8s_batch_api
        self.scan_backlog = scan_backlog
        self.scan_id = scan_id
        self.env_dict = {}

        common_space_volume_path = Path("/common-space")

        # CONTAINER: SkyScan Server
        self.scanner_server_args = self.get_scanner_server_args(
            common_space_volume_path=common_space_volume_path,
            reco_algo=reco_algo,
            nsides=nsides,
            is_real_event=is_real_event,
            predictive_scanning_threshold=predictive_scanning_threshold,
        )
        scanner_server = KubeAPITools.create_container(
            f"skyscan-server-{scan_id}",
            images.get_skyscan_docker_image(docker_tag),
            self.make_skyscan_server_v1envvars(
                rest_address=rest_address,
                scan_id=scan_id,
            ),
            self.scanner_server_args.split(),
            {common_space_volume_path.name: common_space_volume_path},
            memory=scanner_server_memory,
        )
        self.env_dict["scanner_server"] = [e.to_dict() for e in scanner_server.env]

        # CONTAINER(S): TMS Starter(s)
        tms_starters = []
        for i, cluster in enumerate(request_clusters):
            tms_starters.append(
                KubeAPITools.create_container(
                    f"tms-starter-{i}-{scan_id}",
                    ENV.CLIENTMANAGER_IMAGE_WITH_TAG,
                    env=self.make_tms_starter_v1envvars(
                        rest_address=rest_address,
                        scan_id=scan_id,
                        cluster=cluster,
                        max_pixel_reco_time=max_pixel_reco_time,
                        debug_mode=debug_mode,
                    ),
                    args=self.get_tms_starter_args(
                        common_space_volume_path=common_space_volume_path,
                        docker_tag=docker_tag,
                        memory=memory,
                        request_cluster=cluster,
                        debug_mode=debug_mode,
                    ),
                    volumes={common_space_volume_path.name: common_space_volume_path},
                    memory=ENV.K8S_CONTAINER_MEMORY_TMS_STARTER,
                )
            )
        self.tms_args_list = [" ".join(c.args) for c in tms_starters]
        self.env_dict["tms_starters"] = [
            [e.to_dict() for e in c.env] for c in tms_starters
        ]

        # job
        self.job_obj = KubeAPITools.kube_create_job_object(
            self.get_job_name(scan_id),
            [scanner_server] + tms_starters,
            ENV.K8S_NAMESPACE,
            volumes=[common_space_volume_path.name],
        )

    @staticmethod
    def get_job_name(scan_id: str) -> str:
        """Get the name of the K8s job (deterministic)."""
        return f"skyscan-{scan_id}"

    @staticmethod
    def get_scanner_server_args(
        common_space_volume_path: Path,
        reco_algo: str,
        nsides: dict[int, int],
        is_real_event: bool,
        predictive_scanning_threshold: float,
    ) -> str:
        """Make the server container args."""
        args = (
            f"python -m skymap_scanner.server "
            f" --reco-algo {reco_algo}"
            f" --cache-dir {common_space_volume_path} "
            # f" --output-dir {common_space_volume_path} "  # output is sent to skydriver
            f" --client-startup-json {common_space_volume_path/'startup.json'} "
            f" --nsides {' '.join(f'{n}:{x}' for n,x in nsides.items())} "  # k1:v1 k2:v2
            f" {'--real-event' if is_real_event else '--simulated-event'} "
            f" --predictive-scanning-threshold {predictive_scanning_threshold} "
        )
        return args

    @staticmethod
    def get_tms_starter_args(
        common_space_volume_path: Path,
        docker_tag: str,
        memory: str,
        request_cluster: schema.Cluster,
        debug_mode: list[DebugMode],
    ) -> list[str]:
        """Make the starter container args.

        This also includes any client args not added by the
        clientmanager.
        """
        args = "python -m clientmanager "

        match request_cluster.orchestrator:
            case "condor":
                args += (
                    f" condor "  # type: ignore[union-attr]
                    f" --collector {request_cluster.location.collector} "
                    f" --schedd {request_cluster.location.schedd} "
                )
                worker_image = images.get_skyscan_cvmfs_singularity_image(docker_tag)
            case "k8s":
                args += (
                    f" k8s "  # type: ignore[union-attr]
                    f" --host {request_cluster.location.host} "
                    f" --namespace {request_cluster.location.namespace} "
                )
                worker_image = images.get_skyscan_docker_image(docker_tag)
            case other:
                raise ValueError(f"Unknown cluster orchestrator: {other}")

        args += (
            f" start "
            f" --n-workers {request_cluster.n_workers} "
            # f" --dryrun"
            # f" --logs-directory "  # see below
            f" --memory {memory} "
            f" --image {worker_image} "
            f" --client-startup-json {common_space_volume_path/'startup.json'} "
            # f" --client-args {client_args} " # only potentially relevant arg is --debug-directory
        )

        if DebugMode.LOGS_DIRECTORY in debug_mode:
            args += f" --logs-directory {common_space_volume_path} "

        return args.split()

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
    ) -> list[kubernetes.client.V1EnvVar]:
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
        env.extend(
            [
                kubernetes.client.V1EnvVar(name=k, value=str(v))
                for k, v in required.items()
            ]
        )

        # 3. add extra env vars, then filter out if 'None'
        prefiltered = {
            "SKYSCAN_PROGRESS_INTERVAL_SEC": ENV.SKYSCAN_PROGRESS_INTERVAL_SEC,
            "SKYSCAN_RESULT_INTERVAL_SEC": ENV.SKYSCAN_RESULT_INTERVAL_SEC,
            "SKYSCAN_MQ_TIMEOUT_TO_CLIENTS": ENV.SKYSCAN_MQ_TIMEOUT_TO_CLIENTS,
            "SKYSCAN_MQ_TIMEOUT_FROM_CLIENTS": ENV.SKYSCAN_MQ_TIMEOUT_FROM_CLIENTS,
            "SKYSCAN_LOG": ENV.SKYSCAN_LOG,
            "SKYSCAN_LOG_THIRD_PARTY": ENV.SKYSCAN_LOG_THIRD_PARTY,
        }
        env.extend(
            [
                kubernetes.client.V1EnvVar(name=k, value=str(v))
                for k, v in prefiltered.items()
                if v is not None
            ]
        )

        # 4. generate & add auth tokens
        tokens = {
            "SKYSCAN_BROKER_AUTH": SkymapScannerJob._get_token_from_keycloak(
                ENV.KEYCLOAK_OIDC_URL,
                ENV.KEYCLOAK_CLIENT_ID_BROKER,
                ENV.KEYCLOAK_CLIENT_SECRET_BROKER,
            ),
            "SKYSCAN_SKYDRIVER_AUTH": SkymapScannerJob._get_token_from_keycloak(
                ENV.KEYCLOAK_OIDC_URL,
                ENV.KEYCLOAK_CLIENT_ID_SKYDRIVER_REST,
                ENV.KEYCLOAK_CLIENT_SECRET_SKYDRIVER_REST,
            ),
        }
        env.extend(
            [
                kubernetes.client.V1EnvVar(name=k, value=str(v))
                for k, v in tokens.items()
            ]
        )

        return env

    @staticmethod
    def make_tms_starter_v1envvars(
        rest_address: str,
        scan_id: str,
        cluster: schema.Cluster,
        max_pixel_reco_time: int,
        debug_mode: list[DebugMode],
    ) -> list[kubernetes.client.V1EnvVar]:
        """Get the environment variables provided to all containers.

        Also, get the secrets' keys & their values.
        """
        LOGGER.debug(f"making tms starter env vars for {scan_id=}")
        env = []

        # 1. start w/ secrets
        # NOTE: the values come from an existing secret in the current namespace
        env.extend(get_cluster_auth_v1envvars(cluster))
        env.extend(get_tms_s3_v1envvars())

        # 2. add required env vars
        required = {
            # broker/mq vars
            "SKYSCAN_BROKER_ADDRESS": ENV.SKYSCAN_BROKER_ADDRESS,
            # skydriver vars
            "SKYSCAN_SKYDRIVER_ADDRESS": rest_address,
            "SKYSCAN_SKYDRIVER_SCAN_ID": scan_id,
            #
            "EWMS_TMS_S3_BUCKET": ENV.EWMS_TMS_S3_BUCKET,
            "EWMS_TMS_S3_URL": ENV.EWMS_TMS_S3_URL,
            #
            "EWMS_PILOT_TASK_TIMEOUT": max_pixel_reco_time,
            #
            "WORKER_K8S_LOCAL_APPLICATION_NAME": ENV.K8S_APPLICATION_NAME,
        }
        env.extend(
            [
                kubernetes.client.V1EnvVar(name=k, value=str(v))
                for k, v in required.items()
            ]
        )

        # 3. add extra env vars, then filter out if 'None'
        prefiltered = {
            "SKYSCAN_MQ_TIMEOUT_TO_CLIENTS": ENV.SKYSCAN_MQ_TIMEOUT_TO_CLIENTS,
            "SKYSCAN_MQ_TIMEOUT_FROM_CLIENTS": ENV.SKYSCAN_MQ_TIMEOUT_FROM_CLIENTS,
            "SKYSCAN_LOG": ENV.SKYSCAN_LOG,
            "SKYSCAN_LOG_THIRD_PARTY": ENV.SKYSCAN_LOG_THIRD_PARTY,
            "EWMS_PILOT_QUARANTINE_TIME": ENV.EWMS_PILOT_QUARANTINE_TIME,
            "EWMS_PILOT_DUMP_TASK_OUTPUT": (
                True if DebugMode.LOGS_DUMP in debug_mode else None
            ),
        }
        env.extend(
            [
                kubernetes.client.V1EnvVar(name=k, value=str(v))
                for k, v in prefiltered.items()
                if v is not None
            ]
        )

        # 4. generate & add auth tokens
        tokens = {
            "SKYSCAN_BROKER_AUTH": SkymapScannerJob._get_token_from_keycloak(
                ENV.KEYCLOAK_OIDC_URL,
                ENV.KEYCLOAK_CLIENT_ID_BROKER,
                ENV.KEYCLOAK_CLIENT_SECRET_BROKER,
            ),
            "SKYSCAN_SKYDRIVER_AUTH": SkymapScannerJob._get_token_from_keycloak(
                ENV.KEYCLOAK_OIDC_URL,
                ENV.KEYCLOAK_CLIENT_ID_SKYDRIVER_REST,
                ENV.KEYCLOAK_CLIENT_SECRET_SKYDRIVER_REST,
            ),
        }
        env.extend(
            [
                kubernetes.client.V1EnvVar(name=k, value=str(v))
                for k, v in tokens.items()
            ]
        )

        return env

    async def enqueue_job(self) -> Any:
        """Enqueue the k8s job onto the Scan Backlog."""
        LOGGER.info(f"enqueuing k8s job for {self.scan_id=}")
        await scan_backlog.enqueue(self.scan_id, self.job_obj, self.scan_backlog)


class SkymapScannerStopperJob:
    """Wraps a Kubernetes job to stop condor cluster(s) w/ Scanner clients."""

    def __init__(
        self,
        k8s_batch_api: kubernetes.client.BatchV1Api,
        scan_id: str,
        clusters: list[schema.Cluster],
    ):
        LOGGER.info(f"making k8s STOPPER job for {scan_id=}")
        self.k8s_batch_api = k8s_batch_api
        self.scan_id = scan_id

        # make a container per cluster
        containers = []
        for i, cluster in enumerate(clusters):
            args = "python -m clientmanager "
            match cluster.orchestrator:
                case "condor":
                    args += (
                        f" condor "  # type: ignore[union-attr]
                        f" --collector {cluster.location.collector} "
                        f" --schedd {cluster.location.schedd} "
                    )
                case "k8s":
                    args += (
                        f" k8s "  # type: ignore[union-attr]
                        f" --host {cluster.location.host} "
                        f" --namespace {cluster.location.namespace} "
                    )
                case other:
                    raise ValueError(f"Unknown cluster orchestrator: {other}")
            args += f" stop --cluster-id {cluster.cluster_id} "

            containers.append(
                KubeAPITools.create_container(
                    f"tms-stopper-{i}-{scan_id}",
                    ENV.CLIENTMANAGER_IMAGE_WITH_TAG,
                    env=get_cluster_auth_v1envvars(cluster),
                    args=args.split(),
                    memory=ENV.K8S_CONTAINER_MEMORY_TMS_STOPPER,
                )
            )

        self.job_obj = KubeAPITools.kube_create_job_object(
            f"tms-stopper-{scan_id}",
            containers,
            ENV.K8S_NAMESPACE,
        )

    def do_job(self) -> Any:
        """Start the k8s job."""
        LOGGER.info(f"starting k8s STOPPER job for {self.scan_id=}")

        # TODO: stop first k8s job (server & clientmanager-starter)

        KubeAPITools.start_job(self.k8s_batch_api, self.job_obj)
