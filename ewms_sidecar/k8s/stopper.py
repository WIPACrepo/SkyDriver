"""For stopping Skymap Scanner clients on a K8s cluster."""


import kubernetes  # type: ignore[import]

from ..config import LOGGER
from . import k8s_tools


def stop(
    namespace: str,
    cluster_id: str,
    k8s_api: kubernetes.client.ApiClient,
) -> None:
    """Main logic."""

    # Remove workers -- may not be instantaneous
    LOGGER.info("Requesting removal...")
    resp = kubernetes.client.BatchV1Api(k8s_api).delete_namespaced_job(
        name=cluster_id,
        namespace=namespace,
        body=kubernetes.client.V1DeleteOptions(
            propagation_policy="Foreground", grace_period_seconds=5
        ),
    )
    LOGGER.info(
        f"Removed workers: {cluster_id} in namespace {namespace} with response {resp.status} "
    )

    # Remove secret -- may not be instantaneous
    resp = kubernetes.client.CoreV1Api(k8s_api).delete_namespaced_secret(
        name=k8s_tools.get_worker_k8s_secret_name(cluster_id),
        namespace=namespace,
        body=kubernetes.client.V1DeleteOptions(
            propagation_policy="Foreground", grace_period_seconds=5
        ),
    )
    LOGGER.info(
        f"Removed secret: {k8s_tools.get_worker_k8s_secret_name(cluster_id)} in namespace {namespace} with response {resp.status} "
    )

    # TODO: get/forward job logs
