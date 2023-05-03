"""Tools for creating K8s job objects for interacting with Skymap Scanner
instances."""


from pathlib import Path
from typing import Any

import kubernetes.client  # type: ignore[import]
from rest_tools.client import ClientCredentialsAuth

from .. import images, types
from ..config import ENV
from ..database import schema
from .utils import KubeAPITools


def get_condor_token_v1envvar() -> kubernetes.client.V1EnvVar:
    """Get the `V1EnvVar` for `CONDOR_TOKEN`."""
    return kubernetes.client.V1EnvVar(
        name="CONDOR_TOKEN",
        value_from=kubernetes.client.V1EnvVarSource(
            secret_key_ref=kubernetes.client.V1SecretKeySelector(
                name=ENV.K8S_SECRET_NAME,
                key="condor_token_sub2",
            )
        ),
    )


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


class SkymapScannerStarterJob:
    """Wraps a Skymap Scanner Kubernetes job with tools to start and manage."""

    def __init__(
        self,
        api_instance: kubernetes.client.BatchV1Api,
        docker_tag: str,
        scan_id: str,
        # scanner
        reco_algo: str,
        nsides: dict[int, int],
        is_real_event: bool,
        predictive_scanning_threshold: float,
        # tms
        memory: str,
        request_clusters: list[types.RequestorInputCluster],
        max_pixel_reco_time: int | None,
        # universal
        debug_mode: bool,
        # env
        rest_address: str,
    ):
        self.api_instance = api_instance
        common_space_volume_path = Path("/common-space")

        # store some data for public access
        self.scanner_server_args = self.get_scanner_server_args(
            common_space_volume_path=common_space_volume_path,
            reco_algo=reco_algo,
            nsides=nsides,
            is_real_event=is_real_event,
            predictive_scanning_threshold=predictive_scanning_threshold,
        )
        self.tms_args_list = list(
            self.get_tms_starter_args(
                common_space_volume_path=common_space_volume_path,
                singularity_image=images.get_skyscan_cvmfs_singularity_image(
                    docker_tag
                ),
                memory=memory,
                request_cluster=c,
                debug_mode=debug_mode,
            )
            for c in request_clusters
        )
        env = self.make_v1_env_vars(
            rest_address=rest_address,
            scan_id=scan_id,
            max_pixel_reco_time=max_pixel_reco_time,
        )
        self.env_dict = {  # promote `e.name` to a key of a dict (instead of an attr in list element)
            e.name: {k: v for k, v in e.to_dict().items() if k != "name"} for e in env
        }

        # containers
        scanner_server = KubeAPITools.create_container(
            f"skyscan-server-{scan_id}",
            images.get_skyscan_docker_image(docker_tag),
            env,
            self.scanner_server_args.split(),
            {common_space_volume_path.name: common_space_volume_path},
            memory=ENV.K8S_CONTAINER_MEMORY_SKYSCAN_SERVER,
        )
        tms_starters = [
            KubeAPITools.create_container(
                f"tms-starter-{i}-{scan_id}",
                ENV.CLIENTMANAGER_IMAGE_WITH_TAG,
                env,
                args.split(),
                {common_space_volume_path.name: common_space_volume_path},
                memory=ENV.K8S_CONTAINER_MEMORY_TMS_STARTER,
            )
            for i, args in enumerate(self.tms_args_list)
        ]
        # job
        self.job_obj = KubeAPITools.kube_create_job_object(
            f"skyscan-{scan_id}",
            [scanner_server] + tms_starters,
            ENV.K8S_NAMESPACE,
            volumes=[common_space_volume_path.name],
        )

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
        singularity_image: Path,
        memory: str,
        request_cluster: types.RequestorInputCluster,
        debug_mode: bool,
    ) -> str:
        """Make the starter container args.

        This also includes any client args not added by the
        clientmanager.
        """
        args = (
            f"python -m clientmanager "
            f" --collector {request_cluster.collector} "
            f" --schedd {request_cluster.schedd} "
            f" start "
            f" --n-jobs {request_cluster.njobs} "
            # f" --dryrun"
            # f" --logs-directory "  # see below
            # f" --accounting-group "
            f" --memory {memory} "
            f" --singularity-image {singularity_image} "
            f" --client-startup-json {common_space_volume_path/'startup.json'} "
            # f" --client-args {client_args} " # only potentially relevant arg is --debug-directory
        )

        if debug_mode:
            args += f" --logs-directory {common_space_volume_path} "  # TODO unique

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
    def make_v1_env_vars(
        rest_address: str,
        scan_id: str,
        max_pixel_reco_time: int | None,
    ) -> list[kubernetes.client.V1EnvVar]:
        """Get the environment variables provided to all containers.

        Also, get the secrets' keys & their values.
        """
        env = []

        # 1. start w/ secrets
        # NOTE: the values come from an existing secret in the current namespace
        env.append(get_condor_token_v1envvar())
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
            "EWMS_PILOT_SUBPROC_TIMEOUT": (
                max_pixel_reco_time
                if max_pixel_reco_time
                else ENV.EWMS_PILOT_SUBPROC_TIMEOUT  # may also be None
            ),
            "EWMS_PILOT_QUARANTINE_TIME": ENV.EWMS_PILOT_QUARANTINE_TIME,
        }
        env.extend(
            [
                kubernetes.client.V1EnvVar(name=k, value=str(v))
                for k, v in prefiltered.items()
                if v  # skip any env vars that are Falsy
            ]
        )

        # 4. generate & add auth tokens
        tokens = {
            "SKYSCAN_BROKER_AUTH": SkymapScannerStarterJob._get_token_from_keycloak(
                ENV.KEYCLOAK_OIDC_URL,
                ENV.KEYCLOAK_CLIENT_ID_BROKER,
                ENV.KEYCLOAK_CLIENT_SECRET_BROKER,
            ),
            "SKYSCAN_SKYDRIVER_AUTH": SkymapScannerStarterJob._get_token_from_keycloak(
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

    def start_job(self) -> Any:
        """Start the k8s job."""
        KubeAPITools.start_job(self.api_instance, self.job_obj)


class SkymapScannerStopperJob:
    """Wraps a Kubernetes job to stop condor cluster(s) w/ Scanner clients."""

    def __init__(
        self,
        api_instance: kubernetes.client.BatchV1Api,
        scan_id: str,
        condor_clusters: list[schema.CondorClutser],
    ):
        self.api_instance = api_instance

        # make a container per cluster
        containers = []
        for i, cluster in enumerate(condor_clusters):
            args = (
                f"python -m clientmanager "
                f" --collector {cluster.collector} "
                f" --schedd {cluster.schedd} "
                f" stop "
                f" --cluster-id {cluster.cluster_id} "
            )
            containers.append(
                KubeAPITools.create_container(
                    f"tms-stopper-{i}-{scan_id}",
                    ENV.CLIENTMANAGER_IMAGE_WITH_TAG,
                    env=[get_condor_token_v1envvar()],
                    args=args.split(),
                )
            )

        self.job_obj = KubeAPITools.kube_create_job_object(
            f"tms-stopper-{scan_id}",
            containers,
            ENV.K8S_NAMESPACE,
        )

    def start_job(self) -> Any:
        """Start the k8s job."""

        # TODO: stop first k8s job (server & clientmanager-starter)

        KubeAPITools.start_job(self.api_instance, self.job_obj)