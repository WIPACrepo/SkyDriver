"""An interface to the Kubernetes cluster.

Based on https://blog.pythian.com/how-to-create-kubernetes-jobs-with-python/
"""

from typing import Any

import kubernetes.client  # type: ignore[import]
from kubernetes.client.rest import ApiException  # type: ignore[import]

from .config import ENV, LOGGER


class KubeAPITools:
    """A convenience wrapper around `kubernetes.client`."""

    @staticmethod
    def _kube_delete_empty_pods(
        namespace: str = 'default', phase: str = 'Succeeded'
    ) -> None:
        """Pods are never empty, just completed the lifecycle.

        As such they can be deleted. Pods can be without any running
        container in 2 states: Succeeded and Failed. This call doesn't
        terminate Failed pods by default.
        """
        # The always needed object
        deleteoptions = kubernetes.client.V1DeleteOptions()
        # We need the api entry point for pods
        api_pods = kubernetes.client.CoreV1Api()
        # List the pods
        try:
            pods = api_pods.list_namespaced_pod(
                namespace, include_uninitialized=False, timeout_seconds=60
            )
        except ApiException as e:
            LOGGER.error(
                "Exception when calling CoreV1Api->list_namespaced_pod: %s\n" % e
            )

        for pod in pods.items:
            LOGGER.debug(pod)
            podname = pod.metadata.name
            try:
                if pod.status.phase == phase:
                    api_response = api_pods.delete_namespaced_pod(
                        podname, namespace, deleteoptions
                    )
                    LOGGER.info("Pod: {} deleted!".format(podname))
                    LOGGER.debug(api_response)
                else:
                    LOGGER.info(
                        "Pod: {} still not done... Phase: {}".format(
                            podname, pod.status.phase
                        )
                    )
            except ApiException as e:
                LOGGER.error(
                    "Exception when calling CoreV1Api->delete_namespaced_pod: %s\n" % e
                )

    @staticmethod
    def kube_cleanup_finished_jobs(
        api_instance: kubernetes.client.BatchV1Api, namespace: str = 'default'
    ) -> None:
        """Since the TTL flag (ttl_seconds_after_finished) is still in alpha
        (Kubernetes 1.12) jobs need to be cleanup manually As such this method
        checks for existing Finished Jobs and deletes them.

        By default it only cleans Finished jobs. Failed jobs require manual intervention or a second call to this function.

        Docs: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/#clean-up-finished-jobs-automatically

        For deletion you need a new object type! V1DeleteOptions! But you can have it empty!

        CAUTION: Pods are not deleted at the moment. They are set to not running, but will count for your autoscaling limit, so if
                 pods are not deleted, the cluster can hit the autoscaling limit even with free, idling pods.
                 To delete pods, at this moment the best choice is to use the kubectl tool
                 ex: kubectl delete jobs/JOBNAME.
                 But! If you already deleted the job via this API call, you now need to delete the Pod using Kubectl:
                 ex: kubectl delete pods/PODNAME
        """
        deleteoptions = kubernetes.client.V1DeleteOptions()
        try:
            jobs = api_instance.list_namespaced_job(
                namespace, include_uninitialized=False, timeout_seconds=60
            )
        except ApiException as e:
            LOGGER.error(e)
            raise

        # Now we have all the jobs, lets clean up
        # We are also logging the jobs we didn't clean up because they either failed or are still running
        for job in jobs.items:
            LOGGER.debug(job)
            jobname = job.metadata.name
            jobstatus = job.status.conditions
            if job.status.succeeded == 1:
                # Clean up Job
                LOGGER.info(
                    "Cleaning up Job: {}. Finished at: {}".format(
                        jobname, job.status.completion_time
                    )
                )
                try:
                    # What is at work here. Setting Grace Period to 0 means delete ASAP. Otherwise it defaults to
                    # some value I can't find anywhere. Propagation policy makes the Garbage cleaning Async
                    api_response = api_instance.delete_namespaced_job(
                        jobname,
                        namespace,
                        deleteoptions,
                        grace_period_seconds=0,
                        propagation_policy='Background',
                    )
                    LOGGER.debug(api_response)
                except ApiException as e:
                    LOGGER.error(e)
                    raise
            else:
                if jobstatus is None and job.status.active == 1:
                    jobstatus = 'active'
                LOGGER.info(
                    "Job: {} not cleaned up. Current status: {}".format(
                        jobname, jobstatus
                    )
                )

        # Now that we have the jobs cleaned, let's clean the pods
        KubeAPITools._kube_delete_empty_pods(namespace)

    @staticmethod
    def kube_create_job_object(
        name: str,
        containers: list[kubernetes.client.V1Container],
        namespace: str = "default",
    ) -> kubernetes.client.V1Job:
        """Create a k8 Job Object Minimum definition of a job object:

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
        # Body is the object Body
        body = kubernetes.client.V1Job(api_version="batch/v1", kind="Job")
        # Body needs Metadata
        # Attention: Each JOB must have a different name!
        body.metadata = kubernetes.client.V1ObjectMeta(namespace=namespace, name=name)
        # And a Status
        body.status = kubernetes.client.V1JobStatus()
        # Now we start with the Template...
        template = kubernetes.client.V1PodTemplate()
        template.template = kubernetes.client.V1PodTemplateSpec()
        # Make Pod Spec
        template.template.spec = kubernetes.client.V1PodSpec(
            containers=containers, restart_policy='Never'
        )
        # And finaly we can create our V1JobSpec!
        body.spec = kubernetes.client.V1JobSpec(
            ttl_seconds_after_finished=600, template=template.template
        )
        return body

    @staticmethod
    def create_container(
        name: str, image: str, env: dict[str, Any]
    ) -> kubernetes.client.V1Container:
        """Make a Container instance."""
        return kubernetes.client.V1Container(
            name=name,
            image=image,
            env=[
                kubernetes.client.V1EnvVar(name=name, value=value)
                for name, value in env.items()
            ],
        )


class SkymapScannerJob:
    """Wraps a Skymap Scanner Kubernetes job with tools to start and manage."""

    def __init__(self, api_instance: kubernetes.client.BatchV1Api, name: str):
        self.api_instance = api_instance
        self.name = name

        # TODO: figure env -- from requestor?
        env = {}  # type: ignore[var-annotated]
        server = KubeAPITools.create_container(name, ENV.SKYSCAN_IMAGE, env)
        client_manager = KubeAPITools.create_container(name, ENV.SKYSCAN_IMAGE, env)
        self.job = KubeAPITools.kube_create_job_object(
            self.name, [server, client_manager]
        )

    def start(self) -> Any:
        """Start the k8s job.

        Returns REST response.
        """
        try:
            api_response = self.api_instance.create_namespaced_job("default", self.job)
            LOGGER.info(api_response)
        except ApiException as e:
            LOGGER.error(e)
            raise
        return api_response


# if __name__ == '__main__':
#     # Testing Credentials
#     kube_test_credentials()
#     # We try to cleanup dead jobs (READ THE FUNCTION CODE!)
#     kube_cleanup_finished_jobs()
#     kube_delete_empty_pods()
#     # Create a couple of jobs
#     for i in range(3):
#         kube_create_job()
#     # This was to test the use of ENV variables.
#     LOGGER.info("Finshed! - ENV: {}".format(os.environ["VAR"]))
#     sys.exit(0)
