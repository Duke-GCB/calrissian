from kubernetes import client, config, watch
import logging
import os

log = logging.getLogger('calrissian.k8s')

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# When running inside a pod, kubernetes puts the namespace in a text file at this location
K8S_NAMESPACE_FILE = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'
POD_NAME_ENV_VARIABLE = 'CALRISSIAN_POD_NAME'

# Namespace to use if not running in cluster
K8S_FALLBACK_NAMESPACE = 'default'

def read_file(path):
    return open(path).read()


def load_config_get_namespace():
    try:
        config.load_incluster_config() # raises if not in cluster
        namespace = read_file(K8S_NAMESPACE_FILE)
    except config.ConfigException:
        config.load_kube_config()
        namespace = K8S_FALLBACK_NAMESPACE
    return namespace


class CalrissianJobException(Exception):
    pass


class KubernetesClient(object):
    def __init__(self):
        self.job_uid = None
        # load_config must happen before instantiating client
        self.namespace = load_config_get_namespace()
        self.core_api_instance = client.CoreV1Api()
        self.batch_api_instance = client.BatchV1Api()

    def _watching_job(self, job):
        return self.job_uid == job.metadata.uid

    def watch_job(self, job):
        log.info('k8s job \'{}\' started'.format(job.metadata.name))
        if self.job_uid is not None:
            raise CalrissianJobException('This client is already observing job {}'.format(self.job_uid))
        self.job_uid = job.metadata.uid

    def _job_succeeded(self, job):
        log.info('k8s job \'{}\' succeeded'.format(job.metadata.name))
        self.job_uid = None

    def _job_failed(self, job):
        log.info('k8s job \'{}\' failed'.format(job.metadata.name))
        self.job_uid = None
        raise CalrissianJobException('Job failed')

    def submit_job(self, job_body):
        # submit the job
        job = self.batch_api_instance.create_namespaced_job(self.namespace, job_body)
        log.info('Created k8s job name {} with id {}'.format(job.metadata.name, job.metadata.uid))
        self.watch_job(job)

    def wait(self):
        w = watch.Watch()
        for event in w.stream(self.batch_api_instance.list_namespaced_job, self.namespace):
            job = event['object']
            if not self._watching_job(job):
                # Not the job we're looking for
                continue
            if job.status.succeeded:
                self._job_succeeded(job)
                w.stop() # stop watching for events, our job is done
                self.batch_api_instance.delete_namespaced_job(job.metadata.name, self.namespace, body=client.V1DeleteOptions())
            if job.status.failed:
                self._job_failed(job)

    def get_pod_for_name(self, pod_name):
        """
        Given a pod name return details about this pod
        :param pod_name: str: name of the pod to read data about
        :return: V1Pod
        """
        pod_name_field_selector='metadata.name={}'.format(pod_name)
        pod_list = self.core_api_instance.list_namespaced_pod(self.namespace, field_selector=pod_name_field_selector)
        if not pod_list.items:
            raise CalrissianJobException("Unable to find pod with name {}".format(pod_name))
        if len(pod_list.items) != 1:
            raise CalrissianJobException("Multiple pods found with name with name {}".format(pod_name))
        return pod_list.items[0]

    def get_current_pod(self):
        """
        Return pod details about the current pod (requires 'CALRISSIAN_POD_NAME' environment variable to be set)
        :return: V1Pod
        """
        pod_name = os.environ.get(POD_NAME_ENV_VARIABLE)
        if not pod_name:
            raise CalrissianJobException("Missing required environment variable ${}".format(POD_NAME_ENV_VARIABLE))
        return self.get_pod_for_name(pod_name)
