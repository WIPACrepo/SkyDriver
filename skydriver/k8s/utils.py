"""An interface to the Kubernetes cluster."""


import json
from pathlib import Path
from typing import Any, Iterator

import kubernetes.client  # type: ignore[import]
from kubernetes.client.rest import ApiException  # type: ignore[import]

from ..config import ENV, LOGGER


class KubeAPITools:
    """A convenience wrapper around `kubernetes.client`."""

    @staticmethod
    def kube_create_job_object(
        name: str,
        containers: list[kubernetes.client.V1Container],
        namespace: str,
        volumes: list[str] | None = None,  # volume names
    ) -> kubernetes.client.V1Job:
        """Create a k8 Job Object Minimum definition of a job object.

        Based on https://blog.pythian.com/how-to-create-kubernetes-jobs-with-python/

        {'api_version': None, - Str
        'kind': None,     - Str
        'metadata': None, - Metada Object
        'spec': None,     -V1JobSpec
        'status': None}   - V1Job Status
        Docs: https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Job.md
        Docs2: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/#writing-a-job-spec

        Also docs are pretty pretty bad. Best way is to ´pip install kubernetes´ and go via the autogenerated code
        And figure out the chain of objects that you need to hold a final valid object So for a job object you need:
        V1Job -> V1ObjectMeta
              -> V1JobStatus
              -> V1JobSpec -> V1PodTemplate -> V1PodTemplateSpec -> V1Container

        Now the tricky part, is that V1Job.spec needs a .template, but not a PodTemplateSpec, as such
        you need to build a PodTemplate, add a template field (template.template) and make sure
        template.template.spec is now the PodSpec.
        Then, the V1Job.spec needs to be a JobSpec which has a template the template.template field of the PodTemplate.
        Failure to do so will trigger an API error.

        Also Containers must be a list!

        Docs3: https://github.com/kubernetes-client/python/issues/589
        """
        if not volumes:
            volumes = []

        # Body is the object Body
        body = kubernetes.client.V1Job(api_version="batch/v1", kind="Job")
        # Body needs Metadata
        # Attention: Each JOB must have a different name!
        body.metadata = kubernetes.client.V1ObjectMeta(
            namespace=namespace,
            name=name,
            labels={
                # https://argo-cd.readthedocs.io/en/stable/user-guide/resource_tracking/
                "app.kubernetes.io/instance": ENV.K8S_APPLICATION_NAME,
            },
            annotations={
                "argocd.argoproj.io/sync-options": "Prune=false"  # don't want argocd to prune this job
            },
        )
        # And a Status
        body.status = kubernetes.client.V1JobStatus()
        # Now we start with the Template...
        template = kubernetes.client.V1PodTemplate()
        template.template = kubernetes.client.V1PodTemplateSpec(
            metadata=kubernetes.client.V1ObjectMeta(
                labels={
                    "app": "scanner-instance",
                },
            ),
        )
        # Make Pod Spec
        template.template.spec = kubernetes.client.V1PodSpec(
            service_account_name=ENV.K8S_SKYSCAN_JOBS_SERVICE_ACCOUNT,
            containers=containers,
            restart_policy="Never",
            volumes=[
                kubernetes.client.V1Volume(
                    name=n, empty_dir=kubernetes.client.V1EmptyDirVolumeSource()
                )
                for n in volumes
            ],
        )
        # And finaly we can create our V1JobSpec!
        body.spec = kubernetes.client.V1JobSpec(
            ttl_seconds_after_finished=ENV.K8S_TTL_SECONDS_AFTER_FINISHED,
            template=template.template,
            backoff_limit=ENV.K8S_BACKOFF_LIMIT,
        )
        return body

    @staticmethod
    def create_container(
        name: str,
        image: str,
        env: list[kubernetes.client.V1EnvVar],
        args: list[str],
        volumes: dict[str, Path] | None = None,
        memory: str = ENV.K8S_CONTAINER_MEMORY_DEFAULT,
    ) -> kubernetes.client.V1Container:
        """Make a Container instance."""
        if not volumes:
            volumes = {}
        return kubernetes.client.V1Container(
            name=name,
            image=image,
            env=env,
            args=args,
            volume_mounts=[
                kubernetes.client.V1VolumeMount(name=vol, mount_path=str(mnt))
                for vol, mnt in volumes.items()
            ],
            resources=kubernetes.client.V1ResourceRequirements(
                limits={
                    "memory": memory,
                    "cpu": "1",
                },
                requests={
                    "memory": memory,
                    "cpu": "1",
                },
            ),
        )

    @staticmethod
    def start_job(
        k8s_batch_api: kubernetes.client.BatchV1Api,
        job_obj: kubernetes.client.V1Job,
    ) -> Any:
        """Start the k8s job.

        Returns REST response.
        """
        if not job_obj:
            raise ValueError("Job object not created")
        try:
            api_response = k8s_batch_api.create_namespaced_job(
                ENV.K8S_NAMESPACE, job_obj
            )
            LOGGER.info(api_response)
        except ApiException as e:
            LOGGER.exception(e)
            raise
        return api_response

    @staticmethod
    def get_pods(
        k8s_core_api: kubernetes.client.CoreV1Api,
        job_name: str,
        namespace: str,
    ) -> Iterator[kubernetes.client.V1Pod]:
        """Get each pod corresponding to the job.

        Raises `ValueError` if there are no pods for the job.
        """
        pods: kubernetes.client.V1PodList = k8s_core_api.list_namespaced_pod(
            namespace=namespace, label_selector=f"job-name={job_name}"
        )
        if not pods.items:
            raise ValueError(f"Job {job_name} has no pods")
        for pod in pods.items:
            yield pod

    @staticmethod
    def get_pod_status(
        k8s_batch_api: kubernetes.client.BatchV1Api,
        job_name: str,
        namespace: str,
    ) -> dict[str, dict[str, Any]]:
        """Get the status of the k8s pod(s) and their containers.

        Raises `ValueError` if there are no pods for the job.
        """
        LOGGER.info(f"getting pod status for {job_name=} {namespace=}")
        status = {}

        k8s_core_api = kubernetes.client.CoreV1Api(api_client=k8s_batch_api.api_client)

        for pod in KubeAPITools.get_pods(k8s_core_api, job_name, namespace):
            pod_status: kubernetes.client.V1PodStatus = pod.status
            # pod status has non-serializable things like datetime objects
            serializable = json.loads(json.dumps(pod_status.to_dict(), default=str))
            status[pod.metadata.name] = serializable

        return status

    @staticmethod
    def get_container_logs(
        k8s_batch_api: kubernetes.client.BatchV1Api,
        job_name: str,
        namespace: str,
    ) -> dict[str, dict[str, str]]:
        """Grab the logs for all containers.

        Raises `ValueError` if there are no pods for the job.
        """
        LOGGER.info(f"getting logs for {job_name=} {namespace=}")
        logs = {}

        k8s_core_api = kubernetes.client.CoreV1Api(api_client=k8s_batch_api.api_client)

        for pod in KubeAPITools.get_pods(k8s_core_api, job_name, namespace):
            these_logs = {}
            for container in pod.spec.containers:
                these_logs[container.name] = k8s_core_api.read_namespaced_pod_log(
                    pod.metadata.name,
                    namespace,
                    container=container.name,
                    timestamps=True,
                )
            logs[pod.metadata.name] = these_logs

        return logs
