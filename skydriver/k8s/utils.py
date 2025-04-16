"""An interface to the Kubernetes cluster."""

import asyncio
import itertools
import logging
from typing import Any, Iterator

import kubernetes.client  # type: ignore[import-untyped]

from ..config import ENV, sdict

LOGGER = logging.getLogger(__name__)


def was_denied_by_job_quota(e: Exception) -> bool:
    """Did the job fail to start because the k8s namespace job quota was exceeded.

    This stoppage will go away if we try again in a bit.
    """

    # did the job exceed the job quota? if so, there will be fewer jobs in the future
    # ex: kubernetes.utils.create_from_yaml.FailToCreateError: Error from server (Forbidden): {"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"jobs.batch \"skyscan-67af75da614147fe8a740bb96f4be08e\" is forbidden: exceeded quota: skydriver-dev-job-quota, requested: count/jobs.batch=1, used: count/jobs.batch=100, limited: count/jobs.batch=100","reason":"Forbidden","details":{"name":"skyscan-67af75da614147fe8a740bb96f4be08e","group":"batch","kind":"jobs"},"code":403}
    if isinstance(e, kubernetes.utils.FailToCreateError) and bool(
        f"exceeded quota: {ENV.K8S_APPLICATION_NAME}-job-quota" in str(e)
    ):
        return True

    # fall-through
    return False


class KubeAPITools:
    """A convenience wrapper around `kubernetes.client`."""

    @staticmethod
    async def start_job(
        k8s_batch_api: kubernetes.client.BatchV1Api,
        job_dict: sdict,
        inf_retry_if_denied_by_job_quota: bool = False,
        logger: logging.Logger = LOGGER,
    ) -> Any:
        """Start the k8s job.

        Returns REST response.
        """
        if not job_dict:
            raise ValueError("No job object to create")

        for i in itertools.count():
            logger.info(f"K8s Job (attempt #{i + 1}):")
            logger.info(job_dict)
            try:
                resp = kubernetes.utils.create_from_dict(
                    k8s_batch_api.api_client,
                    job_dict,
                    namespace=ENV.K8S_NAMESPACE,
                )
                logger.info("k8s job successfully created!")
                return resp
            except Exception as e:  # broad b/c re-raising
                if inf_retry_if_denied_by_job_quota and was_denied_by_job_quota(e):
                    logger.warning(
                        f"k8s job request was denied b/c the k8s namespace's job quota "
                        f"is currently at max (attempt #{i+1}) -- "
                        f"trying again in {ENV.K8S_START_JOB_RETRY_FOR_JOB_QUOTA_DELAY}s"
                        f": {repr(e)} -> {str(e)}"
                    )
                    # maybe next time, it'll be ok
                    await asyncio.sleep(ENV.K8S_START_JOB_RETRY_FOR_JOB_QUOTA_DELAY)
                    continue
                else:
                    logger.error("request to make k8s job failed (see above job_dict)")
                    raise

    @staticmethod
    def get_pods(
        k8s_core_api: kubernetes.client.CoreV1Api,
        job_name: str,
        namespace: str,
    ) -> Iterator[str]:
        """Get each pod corresponding to the job.

        Raises `ValueError` if there are no pods for the job.
        """
        pods: kubernetes.client.V1PodList = k8s_core_api.list_namespaced_pod(
            namespace=namespace, label_selector=f"job-name={job_name}"
        )
        if not pods.items:
            raise ValueError(f"Job {job_name} has no pods")
        for pod in pods.items:
            yield pod.metadata.name
