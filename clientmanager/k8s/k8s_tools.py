"""An interface to the Kubernetes cluster."""


import kubernetes.client  # type: ignore[import]

from ..config import ENV, LOCAL_K8S_HOST, LOGGER


def get_worker_k8s_secret_name(cluster_id: str) -> str:
    return f"{cluster_id}-secret"


def patch_or_create_namespaced_secret(
    core_api: kubernetes.client.CoreV1Api,
    host: str,
    namespace: str,
    secret_name: str,
    secret_type: str,
    encoded_secret_data: dict[str, str],
) -> None:
    """Patch secret and if not exist create."""

    if host == LOCAL_K8S_HOST:
        metadata = kubernetes.client.V1ObjectMeta(
            name=secret_name,
            labels={
                # https://argo-cd.readthedocs.io/en/stable/user-guide/resource_tracking/
                "app.kubernetes.io/instance": ENV.WORKER_K8S_LOCAL_APPLICATION_NAME,
            },
            annotations={
                "argocd.argoproj.io/sync-options": "Prune=false"  # don't want argocd to prune this job
            },
        )
    else:
        metadata = kubernetes.client.V1ObjectMeta(name=secret_name)

    # Instantiate the Secret object
    body = kubernetes.client.V1Secret(
        data=encoded_secret_data,
        type=secret_type,
        metadata=metadata,
    )

    # try to patch first
    try:
        core_api.patch_namespaced_secret(secret_name, namespace, body)
        LOGGER.info(f"Secret {secret_name} in namespace {namespace} has been patched")
    except kubernetes.client.rest.ApiException as e:
        # a (None or 404) means we can create secret instead, see below
        if e.status and e.status != 404:
            LOGGER.exception(e)
            raise

    # create if patch failed
    try:
        core_api.create_namespaced_secret(namespace=namespace, body=body)
        LOGGER.info(
            f"Created secret {secret_name} of type {secret_type} in namespace {namespace}"
        )
    except kubernetes.client.rest.ApiException as e:
        LOGGER.exception(e)
        raise
