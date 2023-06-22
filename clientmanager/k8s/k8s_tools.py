"""An interface to the Kubernetes cluster."""


import kubernetes.client  # type: ignore[import]

from ..config import LOGGER


def patch_or_create_namespaced_secret(
    api_instance: kubernetes.client.BatchV1Api,
    namespace: str,
    secret_name: str,
    secret_type: str,
    encoded_secret_data: dict[str, str],
) -> None:
    """Patch secret and if not exist create."""
    # Instantiate the Secret object
    body = kubernetes.client.V1Secret(
        data=encoded_secret_data,
        type=secret_type,
        metadata=kubernetes.client.V1ObjectMeta(name=secret_name),
    )

    # try to patch first
    try:
        api_instance.patch_namespaced_secret(secret_name, namespace, body)
        LOGGER.info(f"Secret {secret_name} in namespace {namespace} has been patched")
    except kubernetes.client.rest.ApiException as e:
        # a (None or 404) means we can create secret instead, see below
        if e.status and e.status != 404:
            LOGGER.exception(e)
            raise

    # create if patch failed
    try:
        api_instance.create_namespaced_secret(namespace=namespace, body=body)
        LOGGER.info(
            f"Created secret {secret_name} of type {secret_type} in namespace {namespace}"
        )
    except kubernetes.client.rest.ApiException as e:
        LOGGER.exception(e)
        raise
