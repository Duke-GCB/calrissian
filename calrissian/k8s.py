from kubernetes import client, config, watch
import logging
import os

log = logging.getLogger('calrissian.k8s')

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# When running inside a pod, kubernetes puts the namespace in a text file at this location
K8S_NAMESPACE_FILE = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'

# Environment variable that will receive the name of this pod so we can lookup volume details
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
        self.job = None
        # load_config must happen before instantiating client
        self.process_exit_code = None
        self.namespace = load_config_get_namespace()
        self.core_api_instance = client.CoreV1Api()
        self.batch_api_instance = client.BatchV1Api()

    def submit_job(self, job_body):
        job = self.batch_api_instance.create_namespaced_job(self.namespace, job_body)
        log.info('Created k8s job name {} with id {}'.format(job.metadata.name, job.metadata.uid))
        self._set_job(job)

    def wait_for_completion(self):
        w = watch.Watch()
        for event in w.stream(self.core_api_instance.list_namespaced_pod, self.namespace, label_selector=self._get_pod_label_selector()):
            pod = event['object']
            status = self.get_first_status_or_none(pod.status.container_statuses)
            if status is None:
                continue
            if self.state_is_running(status.state):
                continue
            elif self.state_is_terminated(status.state):
                self._handle_terminated_state(status.state)
                self.batch_api_instance.delete_namespaced_job(self.job.metadata.name, self.namespace, body=client.V1DeleteOptions(propagation_policy='Background'))
                self._clear_job()
                # stop watching for events, our job is done. Causes wait loop to exit
                w.stop()
            else:
                raise CalrissianJobException('Unexpected pod container status', status)
        return self.process_exit_code

    def _set_job(self, job):
        log.info('k8s job \'{}\' started'.format(job.metadata.name))
        if self.job is not None:
            raise CalrissianJobException('This client is already observing job {}'.format(self.job))
        self.job = job

    def _clear_job(self):
        self.job = None

    def _get_pod_label_selector(self):
        # We list pods by their controller uid, which should match our job uid
        return 'controller-uid={}'.format(self.job.metadata.uid)

    @staticmethod
    def state_is_running(state):
        return state.running or state.waiting

    @staticmethod
    def state_is_terminated(state):
        return state.terminated

    @staticmethod
    def get_first_status_or_none(container_statuses):
        """
        Check the container statuses list. Should be 0 or 1 items. If 0, there's no container yet. If 1, there's a
        container. If > 1, there's more than 1 container and that's unexpected behavior
        :param container_statuses: list of V1ContainerStatus
        :return: V1ContainerStatus if len of list is 1, None if 0, and raises CalrissianJobException if > 1
        """
        if not container_statuses: # None or empty list
            return None
        elif len(container_statuses) > 1:
            raise CalrissianJobException(
                'Expected 0 or 1 container statuses in job, found {}'.format(len(container_statuses), container_statuses))
        else:
            return container_statuses[0]

    def _handle_terminated_state(self, state):
        """
        Sets self.process_exit_code to the exit code from a terminated container
        :param status: V1ContainerState
        :return: None
        """
        # Extract the exit code out of the status
        log.info('setting process_exit_code from state {}'.format(state))
        self.process_exit_code = state.terminated.exit_code

    def get_pod_for_name(self, pod_name):
        """
        Given a pod name return details about this pod
        :param pod_name: str: name of the pod to read data about
        :return: V1Pod
        """
        pod_name_field_selector = 'metadata.name={}'.format(pod_name)
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
