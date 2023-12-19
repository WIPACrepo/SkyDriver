"""Tools for creating K8s job objects for interacting with Skymap Scanner
instances."""


import uuid
from pathlib import Path
from typing import Any

import kubernetes.client  # type: ignore[import-untyped]
from rest_tools.client import ClientCredentialsAuth

from .. import images
from ..config import (
    CLUSTER_STOPPER_K8S_JOB_N_RETRIES,
    CLUSTER_STOPPER_K8S_TTL_SECONDS_AFTER_FINISHED,
    ENV,
    K8S_CONTAINER_MEMORY_CLUSTER_STARTER_BYTES,
    K8S_CONTAINER_MEMORY_CLUSTER_STOPPER_BYTES,
    LOGGER,
    DebugMode,
)
from ..database import schema
from .utils import KubeAPITools


def get_cluster_auth_v1envvars(
    cluster: schema.Cluster,
) -> list[kubernetes.client.V1EnvVar]:
    """Get the `V1EnvVar`s for workers' auth."""
    LOGGER.debug(f"getting auth secret env vars for {cluster=}")
    _, info = cluster.to_known_cluster()
    return info["v1envvars"]  # type: ignore[no-any-return]


def get_cluster_starter_s3_v1envvars() -> list[kubernetes.client.V1EnvVar]:
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


class SkymapScannerK8sWrapper:
    """Wraps a Skymap Scanner Kubernetes job with tools to start and manage."""

    def __init__(
        self,
        #
        docker_tag: str,
        scan_id: str,
        # scanner
        scanner_server_memory_bytes: int,
        reco_algo: str,
        nsides: dict[int, int],
        is_real_event: bool,
        predictive_scanning_threshold: float,
        # cluster starter
        starter_exc: str,  # TODO - remove once tested in prod
        worker_memory_bytes: int,
        worker_disk_bytes: int,
        request_clusters: list[schema.Cluster],
        max_pixel_reco_time: int,
        max_worker_runtime: int,
        priority: int,
        # universal
        debug_mode: list[DebugMode],
        # env
        rest_address: str,
        skyscan_mq_client_timeout_wait_for_first_message: int | None,
    ):
        LOGGER.info(f"making k8s job for {scan_id=}")
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
                skyscan_mq_client_timeout_wait_for_first_message=skyscan_mq_client_timeout_wait_for_first_message,
            ),
            self.scanner_server_args.split(),
            cpu=1,
            volumes={common_space_volume_path.name: common_space_volume_path},
            memory=scanner_server_memory_bytes,
        )
        self.env_dict["scanner_server"] = [e.to_dict() for e in scanner_server.env]

        # CONTAINER(S): Cluster Starter(s)
        tms_starters = []
        for i, cluster in enumerate(request_clusters):
            tms_starters.append(
                KubeAPITools.create_container(
                    f"{starter_exc}-{i}-{scan_id}",  # TODO - replace once tested in prod
                    ENV.THIS_IMAGE_WITH_TAG,
                    env=self.make_cluster_starter_v1envvars(
                        rest_address=rest_address,
                        scan_id=scan_id,
                        cluster=cluster,
                        max_pixel_reco_time=max_pixel_reco_time,
                        debug_mode=debug_mode,
                    ),
                    args=self.get_cluster_starter_args(
                        starter_exc=starter_exc,  # TODO - remove once tested in prod
                        common_space_volume_path=common_space_volume_path,
                        docker_tag=docker_tag,
                        worker_memory_bytes=worker_memory_bytes,
                        worker_disk_bytes=worker_disk_bytes,
                        request_cluster=cluster,
                        debug_mode=debug_mode,
                        max_worker_runtime=max_worker_runtime,
                        priority=priority,
                    ),
                    cpu=0.125,
                    volumes={common_space_volume_path.name: common_space_volume_path},
                    memory=K8S_CONTAINER_MEMORY_CLUSTER_STARTER_BYTES,
                )
            )
        self.cluster_starter_args_list = [" ".join(c.args) for c in tms_starters]
        self.env_dict["tms_starters"] = [
            [e.to_dict() for e in c.env] for c in tms_starters
        ]

        # job
        self.job_obj = KubeAPITools.kube_create_job_object(
            self.get_job_name(scan_id),
            [scanner_server] + tms_starters,
            ENV.K8S_NAMESPACE,
            ENV.K8S_TTL_SECONDS_AFTER_FINISHED,
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
    def get_cluster_starter_args(
        starter_exc: str,  # TODO - remove once tested in prod
        common_space_volume_path: Path,
        docker_tag: str,
        worker_memory_bytes: int,
        worker_disk_bytes: int,
        request_cluster: schema.Cluster,
        debug_mode: list[DebugMode],
        max_worker_runtime: int,
        priority: int,
    ) -> list[str]:
        """Make the starter container args."""
        args = f"python -m clientmanager --uuid {str(uuid.uuid4().hex)}"

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
            # f" --spool "  # see below
            f" --worker-memory-bytes {worker_memory_bytes} "
            f" --worker-disk-bytes {worker_disk_bytes} "
            f" --image {worker_image} "
            f" --client-startup-json {common_space_volume_path/'startup.json'} "
            # f" --client-args {client_args} " # only potentially relevant arg is --debug-directory
            f" --max-worker-runtime {max_worker_runtime}"
            f" --priority {priority}"
        )

        if DebugMode.CLIENT_LOGS in debug_mode:
            args += " --spool "

        # ADAPT args for EWMS Sidecar
        # TODO - remove once tested in prod
        if starter_exc == "ewms_sidecar" and request_cluster.orchestrator == "condor":
            args.replace("clientmanager", "ewms_sidecar remote-condor")
            args.replace(" condor ", " ")
            args.replace(" start ", " ")

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
        skyscan_mq_client_timeout_wait_for_first_message: int | None,
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
        env.extend(
            [
                kubernetes.client.V1EnvVar(name=k, value=str(v))
                for k, v in prefiltered.items()
                if v is not None
            ]
        )

        # 4. generate & add auth tokens
        tokens = {
            "SKYSCAN_BROKER_AUTH": SkymapScannerK8sWrapper._get_token_from_keycloak(
                ENV.KEYCLOAK_OIDC_URL,
                ENV.KEYCLOAK_CLIENT_ID_BROKER,
                ENV.KEYCLOAK_CLIENT_SECRET_BROKER,
            ),
            "SKYSCAN_SKYDRIVER_AUTH": SkymapScannerK8sWrapper._get_token_from_keycloak(
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
    def make_cluster_starter_v1envvars(
        rest_address: str,
        scan_id: str,
        cluster: schema.Cluster,
        max_pixel_reco_time: int,
        debug_mode: list[DebugMode],
    ) -> list[kubernetes.client.V1EnvVar]:
        """Get the environment variables provided to all containers.

        Also, get the secrets' keys & their values.
        """
        LOGGER.debug(f"making cluster starter env vars for {scan_id=}")
        env = []

        # 1. start w/ secrets
        # NOTE: the values come from an existing secret in the current namespace
        env.extend(get_cluster_auth_v1envvars(cluster))
        env.extend(get_cluster_starter_s3_v1envvars())

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
            #
            "SKYSCAN_LOG": ENV.SKYSCAN_LOG,
            "SKYSCAN_LOG_THIRD_PARTY": ENV.SKYSCAN_LOG_THIRD_PARTY,
            #
            "SKYSCAN_EWMS_PILOT_LOG": "WARNING",  # default is too low
            "SKYSCAN_MQ_CLIENT_LOG": "WARNING",  # default is too low
            #
            "EWMS_PILOT_QUARANTINE_TIME": ENV.EWMS_PILOT_QUARANTINE_TIME,
            "EWMS_PILOT_DUMP_TASK_OUTPUT": (
                True if DebugMode.CLIENT_LOGS in debug_mode else None
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
            "SKYSCAN_BROKER_AUTH": SkymapScannerK8sWrapper._get_token_from_keycloak(
                ENV.KEYCLOAK_OIDC_URL,
                ENV.KEYCLOAK_CLIENT_ID_BROKER,
                ENV.KEYCLOAK_CLIENT_SECRET_BROKER,
            ),
            "SKYSCAN_SKYDRIVER_AUTH": SkymapScannerK8sWrapper._get_token_from_keycloak(
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


class SkymapScannerWorkerStopperK8sWrapper:
    """Wraps K8s logic to stop workers of a Skymap Scanner instance."""

    def __init__(
        self,
        k8s_batch_api: kubernetes.client.BatchV1Api,
        scan_id: str,
        clusters: list[schema.Cluster],
    ):
        self.k8s_batch_api = k8s_batch_api
        self.scan_id = scan_id

        # make a container per cluster
        containers = []
        for i, cluster in enumerate(clusters):
            args = f"python -m clientmanager --uuid {cluster.uuid}"
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
                    f"cluster-stopper-{i}-{scan_id}",
                    ENV.THIS_IMAGE_WITH_TAG,
                    cpu=0.125,
                    env=get_cluster_auth_v1envvars(cluster),
                    args=args.split(),
                    memory=K8S_CONTAINER_MEMORY_CLUSTER_STOPPER_BYTES,
                )
            )

        if not containers:
            self.worker_stopper_job_obj = None
        else:
            self.worker_stopper_job_obj = KubeAPITools.kube_create_job_object(
                f"cluster-stopper-{scan_id}",
                containers,
                ENV.K8S_NAMESPACE,
                CLUSTER_STOPPER_K8S_TTL_SECONDS_AFTER_FINISHED,
                n_retries=CLUSTER_STOPPER_K8S_JOB_N_RETRIES,
            )

    def go(self) -> Any:
        """Stop all workers of a Skymap Scanner instance."""

        # NOTE - we don't want to stop the first k8s job because its containers will stop themselves.
        #        plus, 'K8S_TTL_SECONDS_AFTER_FINISHED' will allow logs & pod status to be retrieved for some time
        #
        # stop first k8s job (server & cluster starters) -- may not be instantaneous
        # LOGGER.info(
        #     f"requesting removal of Skymap Scanner Job (server & cluster starters) -- {self.scan_id=}..."
        # )
        # resp = self.k8s_batch_api.delete_namespaced_job(
        #     name=SkymapScannerK8sWrapper.get_job_name(self.scan_id),
        #     namespace=ENV.K8S_NAMESPACE,
        #     body=kubernetes.client.V1DeleteOptions(
        #         propagation_policy="Foreground", grace_period_seconds=5
        #     ),
        # )
        # LOGGER.info(
        #     f"removed Skymap Scanner Job {self.scan_id=} -- with response {resp.status} "
        # )

        # stop workers
        if self.worker_stopper_job_obj:
            LOGGER.info(f"starting k8s CLUSTER-STOPPER job for {self.scan_id=}")
            KubeAPITools.start_job(self.k8s_batch_api, self.worker_stopper_job_obj)
        else:
            LOGGER.info(f"no workers to stop for {self.scan_id=}")
