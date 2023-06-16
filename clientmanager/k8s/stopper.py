"""For stopping Skymap Scanner clients on a K8s cluster."""


import kubernetes  # type: ignore[import]

from ..config import LOGGER


def stop(
    namespace: str,
    cluster_id: str,
    k8s_client: kubernetes.client.ApiClient,
) -> None:
    """Main logic."""
    LOGGER.info(f"Stopping Skymap Scanner client workers on {1}")

    # Remove workers -- may not be instantaneous
    LOGGER.info("Requesting removal...")
    k8s_response = k8s_client.delete_namespaced_job(
        name=cluster_id,
        namespace=namespace,
        body=kubernetes.client.V1DeleteOptions(
            propagation_policy="Foreground", grace_period_seconds=5
        ),
    )
    LOGGER.debug("Job deleted. status='%s'" % str(k8s_response.status))
    LOGGER.info(
        f"Removed workers: {cluster_id} in namespace {namespace} with response {k8s_response.status} "
    )

    # TODO: get/forward job logs
