"""Init."""

from .act import act  # noqa: F401


def get_worker_k8s_secret_name(cluster_id: str) -> str:
    return f"{cluster_id}-secret"
