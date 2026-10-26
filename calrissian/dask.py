import os
import logging
import yaml

from typing import List, Optional, Union

from kubernetes import client, watch
from kubernetes.client.rest import ApiException
from kubernetes.client.models import V1Container, V1ContainerStatus
from urllib3.exceptions import HTTPError
from cwltool.utils import CWLObjectType

from calrissian.executor import IncompleteStatusException
from calrissian.retry import retry_exponential_if_exception_type
from calrissian.job import (
    CalrissianCommandLineJob,
    KubernetesPodBuilder
)
from calrissian.job import (
    quoted_arg_list,
    random_tag,
    k8s_safe_name,
)
from calrissian.job import (
    DEFAULT_INIT_IMAGE,
    INIT_IMAGE_ENV_VARIABLE
)
from calrissian.k8s import (
    CalrissianJobException,
    CompletionResult,
    KubernetesClient,
    PodMonitor
)

log = logging.getLogger("calrissian.dask")
log_main = logging.getLogger("calrissian.main")


def dask_req_validate(requirement: Optional[CWLObjectType]) -> bool:
    """
    Check if CWL ext DaskGatewayRequirements has all the required keys
    - If a key is missing return False.
    - Otherwise, return True.
    """
    if requirement is None:
        return False
    
    required_keys = ["workerCores", 
                     "workerCoresLimit", 
                     "workerMemory", 
                     "clusterMaxCores", 
                     "clusterMaxMemory", 
                     "class"]
    
    return set(required_keys).issubset(requirement.keys())


class KubernetesDaskPodBuilder(KubernetesPodBuilder):
    
    def __init__(self, dask_gateway_url, dask_gateway_controller, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.dask_gateway_url = dask_gateway_url
        self.dask_gateway_controller = dask_gateway_controller
    
        self.dask_requirement = next((elem for elem in self.requirements if elem['class'] == 'https://calrissian-cwl.github.io/schema#DaskGatewayRequirement'), None)
    

    def container_args(self):
        return ['set -e; trap "touch /shared/completed" EXIT;export DASK_CLUSTER=$(cat /shared/dask_cluster_name.txt) ; ' + super().container_args()[0]]


    def init_container_command(self):
        """
        Constructs the command for initializing a Dask cluster.
        """
        if self.dask_gateway_controller:
            return ['python', '/controller/init-dask.py']
        src = os.path.join(os.path.dirname(__file__), "dask/init-dask.py")
        with open(src, 'r') as f:
            file_content = f.read()
        return ['python', '-c', file_content]


    def sidecar_container_command(self):
        """
        Constructs the command for disposing of a Dask cluster.
        """
        if self.dask_gateway_controller:
            return ['python', '/controller/dispose-dask.py']
        src = os.path.join(os.path.dirname(__file__), "dask/dispose-dask.py")

        with open(src, 'r') as f:
            file_content = f.read()
        return ['python', '-c', file_content]


    def container_environment(self):
        environment = []
        for name, value in sorted(self.environment.items()):
            environment.append({'name': name, 'value': value})
        
        environment.append({'name': 'DASK_GATEWAY_WORKER_CORES', 'value': str(self.dask_requirement.get("workerCores"))})
        environment.append({'name': 'DASK_GATEWAY_WORKER_MEMORY', 'value': str(self.dask_requirement.get("workerMemory"))})
        environment.append({'name': 'DASK_GATEWAY_WORKER_CORES_LIMIT', 'value': str(self.dask_requirement.get("workerCoresLimit"))})
        environment.append({'name': 'DASK_GATEWAY_CLUSTER_MAX_CORES', 'value': str(self.dask_requirement.get("clusterMaxCores"))})
        environment.append({'name': 'DASK_GATEWAY_CLUSTER_MAX_RAM', 'value': str(self.dask_requirement.get("clusterMaxMemory"))})
        environment.append({'name': 'DASK_GATEWAY_URL', 'value': str(self.dask_gateway_url)})
        environment.append({'name': 'DASK_GATEWAY_IMAGE', 'value': str(self.container_image)})
        environment.append({'name': 'DASK_CLUSTER_NAME_PATH', 'value': '/shared/dask_cluster_name.txt'})

        return environment


    def init_containers(self):
        containers = []
        # get dirname for any actual paths
        dirs_to_create = [os.path.dirname(p) for p in [self.stdout, self.stderr] if p]
        # Remove empty strings
        dirs_to_create = [d for d in dirs_to_create if d]
        # Quote if necessary
        dirs_to_create = quoted_arg_list(dirs_to_create)
        command_list = ['mkdir -p {};'.format(d) for d in dirs_to_create]
        if command_list:
            containers.append({
                'name': self.init_container_name(),
                'image':  os.environ.get(INIT_IMAGE_ENV_VARIABLE, DEFAULT_INIT_IMAGE),
                'command': ['/bin/sh', '-c', ' '.join(command_list)],
                'workingDir': self.container_workingdir(),
                'volumeMounts': self.volume_mounts,
            })

        init_dask_cluster = {
            'name': self.init_container_name(),
            'image': str(self.container_image),
            'env': self.container_environment(),
            'command': self.init_container_command(),
            'workingDir': self.container_workingdir(),
            'volumeMounts': self.volume_mounts
        }

        containers.append(init_dask_cluster)

        return containers


    def build(self):

        spec = {
            'metadata': {
                'name': self.pod_name(),
                'labels': self.pod_labels(),
            },
            'apiVersion': 'v1',
            'kind':'Pod',
            'spec': {
                'initContainers': self.init_containers(),
                'containers': [
                    {
                        'name': 'main-container',
                        'image': str(self.container_image),
                        'command': self.container_command(),
                        'args': self.container_args(),
                        'env': self.container_environment(),
                        'resources': self.container_resources(),
                        'volumeMounts': self.volume_mounts,
                        'workingDir': self.container_workingdir(),
                    },
                    {
                        'name': 'sidecar-container',
                        'image': str(self.container_image),
                        'command': self.sidecar_container_command(),
                        'env': self.container_environment(),
                        'resources': self.container_resources(),
                        'volumeMounts': self.volume_mounts,
                        'workingDir': self.container_workingdir(),
                        }
                ],
                'restartPolicy': 'Never',
                'volumes': self.volumes,
                'securityContext': self.security_context,
                'nodeSelector': self.select_pod_nodeselectors()
            }
        }
        
        if ( self.serviceaccount ):
            spec['spec']['serviceAccountName'] = self.serviceaccount
        
        if ( self.priority_class ):
            spec['spec']['priorityClassName'] = self.priority_class

        if self.env_from_secret or self.env_from_configmap:
            envfrom = []

            if self.env_from_secret:
                envfrom.extend(self.pod_envfromsecret())

            if self.env_from_configmap:
                envfrom.extend(self.pod_envfromconfigmap())

            spec['spec']['containers'][0]["envFrom"] = envfrom

        return spec
    

class CalrissianCommandLineDaskJob(CalrissianCommandLineJob):

    container_shared_dir = '/shared'
    
    dask_gateway_controller_dir = '/controller'

    dask_gateway_config_dir = '/etc/dask'

    def __init__(self, *args, **kwargs):
        super(CalrissianCommandLineDaskJob, self).__init__(*args, **kwargs)
        self.client = KubernetesDaskClient()

        self.dask_cm_name = self.dask_configmap_name()

    def dask_configmap_name(self):
        tag = random_tag()
        return k8s_safe_name('{}-cm-{}'.format('dask', tag))

    def wait_for_kubernetes_pod(self, cm_name: Optional[str] = None):
        return self.client.wait_for_completion(cm_name=cm_name)

    def get_dask_script_cm_name(self, runtimeContext):
        return runtimeContext.dask_script_configmap

    def get_dask_gateway_url(self, runtimeContext):
        return runtimeContext.dask_gateway_url

    def _add_configmap_volume_and_binding(self, name, cm_name, target):
        self.volume_builder.add_configmap_volume(name, cm_name)
        self.volume_builder.add_configmap_volume_binding(name, target)

    def create_kubernetes_runtime(self, runtimeContext):
        # In cwltool, the runtime list starts as something like ['docker','run'] and these various builder methods
        # append to that list with docker (or singularity) options like volume mount paths
        # As we build up kubernetes, these aren't really used this way so we leave it empty
        runtime = []

        # Append volume for outdir
        self._add_volume_binding(os.path.realpath(self.outdir), self.builder.outdir, writable=True)
        # Use a kubernetes emptyDir: {} volume for /tmp
        # Note that below add_volumes() may result in other temporary files being mounted
        # from the calrissian host's tmpdir prefix into an absolute container path, but this will
        # not conflict with '/tmp' as an emptyDir
        self._add_emptydir_volume_and_binding('tmpdir', self.container_tmpdir)

        # Call the ContainerCommandLineJob add_volumes method
        self.add_volumes(self.pathmapper,
                         runtime,
                         tmpdir_prefix=runtimeContext.tmpdir_prefix,
                         secret_store=runtimeContext.secret_store,
                         any_path_okay=True)

        if self.generatemapper is not None:
            # Seems to be true if docker is a hard requirement
            # This evaluates to true if docker_is_required is true
            # Used only for generatemapper add volumes
            any_path_okay = self.builder.get_requirement("DockerRequirement")[1] or False
            self.add_volumes(
                self.generatemapper,
                runtime,
                tmpdir_prefix=runtimeContext.tmpdir_prefix,
                secret_store=runtimeContext.secret_store,
                any_path_okay=any_path_okay)


        self.client.create_dask_gateway_config_map(
            dask_gateway_url=self.get_dask_gateway_url(runtimeContext),
            cm_name=self.dask_cm_name)

        # emptyDir volume at /shared for sharing the Dask cluster name between containers
        self._add_emptydir_volume_and_binding('shared-data', self.container_shared_dir)
        

        # Need this ConfigMap to simplify configuration by providing defaults, 
        # as explained here: https://gateway.dask.org/configuration-user.html
        self._add_configmap_volume_and_binding(
            name=self.dask_cm_name,
            cm_name=self.dask_cm_name,
            target=self.dask_gateway_config_dir)

        dask_gateway_controller_cm_name = self.get_dask_script_cm_name(runtimeContext)

        controller_cm_exists = self.client.get_configmap_from_namespace(name=dask_gateway_controller_cm_name)
        if controller_cm_exists:
            self._add_configmap_volume_and_binding(
                name=dask_gateway_controller_cm_name,
                cm_name=dask_gateway_controller_cm_name,
                target=self.dask_gateway_controller_dir)
        

        k8s_builder = KubernetesDaskPodBuilder(
            self.get_dask_gateway_url(runtimeContext),
            controller_cm_exists,
            self.name,
            self.builder,
            self._get_container_image(),
            self.environment,
            self.volume_builder.volume_mounts,
            self.volume_builder.volumes,
            self.quoted_command_line(),
            self.stdout,
            self.stderr,
            self.stdin,
            self.get_pod_labels(runtimeContext),
            self.get_pod_nodeselectors(runtimeContext),
            self.get_pod_gpu_nodeselectors(runtimeContext),
            self.get_security_context(runtimeContext),
            self.get_pod_serviceaccount(runtimeContext),
            self.get_pod_additional_spec(runtimeContext),
            self.get_no_network_access_pod_labels(runtimeContext),
            self.get_network_access_pod_labels(runtimeContext),
        )

        built = k8s_builder.build()
        log.debug('{}\n{}{}\n'.format('-' * 80, yaml.dump(built), '-' * 80))
        # Report an error if anything was added to the runtime list
        if runtime:
            log.error('Runtime list is not empty. k8s does not use that, so you should see who put something there:\n{}'.format(' '.join(runtime)))
        return built
    
    def execute_kubernetes_pod(self, pod):
        self.client.submit_pod(pod)

    def run(self, runtimeContext, tmpdir_lock=None):
        def get_pod_command(pod):
            return pod['spec']['containers'][0]['args']
            
        def get_pod_name(pod):
            return pod['spec']['containers'][0]['name']

        self.check_requirements(runtimeContext)
        
        if tmpdir_lock:
            with tmpdir_lock:
                self.make_tmpdir()
        else:
            self.make_tmpdir()
        self.populate_env_vars(runtimeContext)

        # specific setup for Kubernetes
        self.setup_kubernetes(runtimeContext)

        self._setup(runtimeContext)
        
        pod = self.create_kubernetes_runtime(runtimeContext) # analogous to create_runtime()
        self.execute_kubernetes_pod(pod) # analogous to _execute()
        completion_result = self.wait_for_kubernetes_pod(cm_name = self.dask_cm_name)
        if completion_result.exit_code != 0:
            log_main.error(f"ERROR the command below failed in pod {get_pod_name(pod)}:")
            log_main.error("\t" + " ".join(get_pod_command(pod)))
        self.finish(completion_result, runtimeContext)


class KubernetesDaskClient(KubernetesClient):

    def __init__(self):
        super().__init__()

    @retry_exponential_if_exception_type((ApiException, HTTPError,), log)
    def submit_pod(self, pod_body):
        with DaskPodMonitor() as monitor:
            pod = self.core_api_instance.create_namespaced_pod(self.namespace, pod_body)
            log.info('Created k8s pod name {} with id {}'.format(pod.metadata.name, pod.metadata.uid))
            monitor.add(pod)
            self._set_pod(pod)


    @retry_exponential_if_exception_type((ApiException, HTTPError,), log)
    def follow_logs(self, status=None):
        pod_name = self.pod.metadata.name

        log.info('[{}] follow_logs start'.format(pod_name))

        kwargs = {
            "follow": True,
            "_preload_content": False,
        }
        if status is not None:
            kwargs["container"] = status.name

        for line in self.core_api_instance.read_namespaced_pod_log(self.pod.metadata.name, self.namespace, **kwargs).stream():
            # .stream() is only available if _preload_content=False
            # .stream() returns a generator, each iteration yields bytes.
            # kubernetes-client decodes them as utf-8 when _preload_content is True
            # https://github.com/kubernetes-client/python/blob/fcda6fe96beb21cd05522c17f7f08c5a7c0e3dc3/kubernetes/client/rest.py#L215-L216
            # So we do the same here
            if status is not None and not status.state.running:
                break

            line = line.decode('utf-8', errors="ignore").rstrip()
            log.debug('[{}] {}'.format(pod_name, line))
            self.tool_log.append(self.format_log_entry(pod_name, line))
        
        log.info('[{}] follow_logs end'.format(pod_name))


    @retry_exponential_if_exception_type((ApiException, HTTPError, IncompleteStatusException), log)
    def wait_for_completion(self, cm_name: str=None) -> CompletionResult:
        w = watch.Watch()
        for event in w.stream(self.core_api_instance.list_namespaced_pod, self.namespace, field_selector=self._get_pod_field_selector()):
            pod = event['object']

            init_status = self.get_list_or_none(pod.status.init_container_statuses)
            if init_status and self.state_is_terminated(init_status[0].state):
                init_status_code = init_status[0].state.terminated.exit_code
                if init_status_code is not None and init_status_code != 0:
                    with DaskPodMonitor() as monitor:
                        self.delete_pod_name(pod.metadata.name)
                        self.delete_configmap_name(cm_name=cm_name)
                        monitor.remove(pod)
                    self._clear_pod()
                    w.stop()
            
            last_status = self.get_last_or_none(pod.status.container_statuses)
            if last_status is None or not self.state_is_terminated(last_status.state):
                statuses = self.get_list_or_none(pod.status.container_statuses)
                if statuses is None:
                    continue
                for status in statuses:
                    log.info('pod name {} with id {} has status {}'.format(pod.metadata.name, pod.metadata.uid, status))
                    if status is None:
                        continue
                    if self.state_is_waiting(status.state):
                        continue
                    elif self.state_is_running(status.state):
                        # Can only get logs once container is running
                        self.follow_logs(status) # This will not return until container completes
                    elif self.state_is_terminated(status.state):
                        continue
                    else:
                        raise CalrissianJobException('Unexpected pod container status', status)
            elif self.state_is_terminated(last_status.state):
                log.info('Handling terminated pod name {} with id {}'.format(pod.metadata.name, pod.metadata.uid))
                container = self.get_container_by_name(pod.spec.containers, 'main-container')
                if container is None:
                    raise CalrissianJobException("Container 'main-container' not found in pod spec", pod)
                node_selectors = self._get_pod_node_selector()
                self._handle_completion(last_status.state, container, node_selectors)
                if self.should_delete_pod():
                    with DaskPodMonitor() as monitor:
                        self.delete_pod_name(pod.metadata.name)
                        self.delete_configmap_name(cm_name=cm_name)
                        monitor.remove(pod)
                self._clear_pod()
                # stop watching for events, our pod is done. Causes wait loop to exit
                w.stop()
            else:
                raise CalrissianJobException('Unexpected pod container status', last_status)
        
        # When the pod is done we should have a completion result
        # Otherwise it will lead to further exceptions
        if self.completion_result is None:
            raise IncompleteStatusException
        
        return self.completion_result
    
    @staticmethod
    def get_list_or_none(container_list: Optional[List[Union[V1ContainerStatus, V1Container]]]) -> Optional[Union[V1ContainerStatus, V1Container]]:
        if not container_list: # None or empty list
            return None
        else:
            return list(container_list)

    @staticmethod
    def get_last_or_none(container_list: Optional[List[Union[V1ContainerStatus, V1Container]]]) -> Optional[Union[V1ContainerStatus, V1Container]]:
        if not container_list: # None or empty list
            return None
        else:
            return container_list[-1]
        
    @staticmethod
    def get_container_by_name(container_list: Optional[List[Union[V1ContainerStatus, V1Container]]], container_name: str) -> Optional[Union[V1ContainerStatus, V1Container]]:
        if not container_list:
            return None

        return next((c for c in container_list if c.name == container_name), None)
        
    @retry_exponential_if_exception_type((ApiException, HTTPError,), log)
    def create_dask_gateway_config_map(self, dask_gateway_url: str, cm_name: str):
        gateway = {'gateway': {'address': dask_gateway_url}}

        configmap = client.V1ConfigMap(
            metadata=client.V1ObjectMeta(name=cm_name),
            data={
                "gateway.yaml": yaml.dump(gateway)
            }
        )

        self.core_api_instance.create_namespaced_config_map(namespace=self.namespace, body=configmap)
    

    def get_configmap_from_namespace(self, name):
        if not name or not isinstance(name, str):
            return False
        try:
            configmap = self.core_api_instance.read_namespaced_config_map(name=name, namespace=self.namespace)
            if configmap:
                return True
            else:
                log.warning(f"ConfigMap '{name}' has no data or does not exist in namespace '{self.namespace}'.")
                return False
        except ApiException as e:
            log.warning(f"Error fetching ConfigMap '{name}' in namespace '{self.namespace}': {e}")
            return False

        except Exception as e:
            log.warning(f"Unexpected error while fetching ConfigMap '{name}': {e}")
            return False


    @retry_exponential_if_exception_type((ApiException, HTTPError,), log)
    def delete_configmap_name(self, cm_name):
        try:
            self.core_api_instance.delete_namespaced_config_map(namespace=self.namespace, name=cm_name)
        except ApiException as e:
            if e.status == 404:
                # configmap was not found - already deleted, so do not retry
                pass
            else:
                # Re-raise
                raise


class DaskPodMonitor(PodMonitor):
    def __init__(self):
        super().__init__()

    @staticmethod
    def cleanup():
        log.info('Starting Cleanup')
        with PodMonitor() as monitor:
            k8s_client = KubernetesDaskClient()
            for pod_name in PodMonitor.pod_names:
                log.info('PodMonitor deleting pod {}'.format(pod_name))
                try:
                    k8s_client.delete_pod_name(pod_name)
                except Exception:
                    log.error('Error deleting pod named {}, ignoring'.format(pod_name))
            PodMonitor.pod_names = []
        log.info('Finishing Cleanup')