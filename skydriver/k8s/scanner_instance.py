"""Tools for creating K8s job objects for interacting with Skymap Scanner
instances."""

import logging
from pathlib import Path

import humanfriendly
import kubernetes.client  # type: ignore[import-untyped]
from rest_tools.client import ClientCredentialsAuth

from .utils import KubeAPITools
from .. import images
from ..config import (
    DebugMode,
    ENV,
)
from ..database import schema

LOGGER = logging.getLogger(__name__)


def get_cluster_auth_v1envvars(
    cluster: schema.InHouseClusterInfo,
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
        # universal
        debug_mode: list[DebugMode],
        # env
        rest_address: str,
        skyscan_mq_client_timeout_wait_for_first_message: int | None,
        scanner_server_env_from_user: dict,
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
                scanner_server_env_from_user=scanner_server_env_from_user,
            ),
            self.scanner_server_args.split(),
            cpu=1,
            volumes={common_space_volume_path.name: common_space_volume_path},
            memory=scanner_server_memory_bytes,
        )
        self.env_dict["scanner_server"] = [e.to_dict() for e in scanner_server.env]

        # s3 uploader
        s3_uploader = KubeAPITools.create_container(
            f"s3-uploader-{scan_id}",
            images.get_skyscan_docker_image(docker_tag),
            [],
            "".split(),
            cpu=0.25,
            volumes={common_space_volume_path.name: common_space_volume_path},
            memory=humanfriendly.parse_size("0.25 G"),
        )

        # job
        self.job_obj = KubeAPITools.kube_create_job_object(
            self.get_job_name(scan_id),
            [scanner_server, s3_uploader],
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

        # 5. Add user's env
        env.extend(
            [
                kubernetes.client.V1EnvVar(name=k, value=str(v))
                for k, v in scanner_server_env_from_user.items()
            ]
        )

        return env
