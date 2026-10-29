from calrissian.scheduling import apply_gpu_tolerations
import os
import logging
import yaml
import threading
from decimal import InvalidOperation
from kubernetes.utils.quantity import parse_quantity

from typing import List, Optional, Union
from datetime import datetime

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
        self.helper_environment = kwargs.pop('helper_environment', None)
        super().__init__(*args, **kwargs)
        if self.helper_environment is None:
            self.helper_environment = getattr(self, 'environment', {})

        self.dask_gateway_url = dask_gateway_url
        self.dask_gateway_controller = dask_gateway_controller
    
        self.dask_requirement = next((elem for elem in self.requirements if elem['class'] == 'https://calrissian-cwl.github.io/schema#DaskGatewayRequirement'), None)
        if self.dask_requirement is not None:
            # Resolve expressions using this job's inputs without changing the
            # shared process requirement used by other workflow invocations.
            self.dask_requirement = {
                key: self.builder.do_eval(value)
                if isinstance(value, str) and ('$(' in value or '${' in value) else value
                for key, value in self.dask_requirement.items()
            }
    

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


    def sidecar_container_resources(self):
        """Read runner settings for the cleanup container at pod-build time."""
        defaults = {
            'requests': {'cpu': '10m', 'memory': '64Mi'},
            'limits': {'cpu': '100m', 'memory': '256Mi'},
        }
        resources = {}
        quantities = {}
        for bound, values in defaults.items():
            resources[bound] = {}
            for resource, default in values.items():
                suffix = 'REQUEST' if bound == 'requests' else 'LIMIT'
                name = f'CALRISSIAN_DASK_CLEANUP_{resource.upper()}_{suffix}'
                value = os.environ.get(name, default).strip()
                try:
                    quantity = parse_quantity(value)
                    if not quantity.is_finite() or quantity <= 0:
                        raise ValueError('must be positive and finite')
                    if resource == 'cpu' and quantity % parse_quantity('1m'):
                        raise ValueError('CPU must be a multiple of 1m')
                except (ValueError, InvalidOperation) as error:
                    raise ValueError(f'Invalid {name}={value!r}: {error}') from error
                resources[bound][resource] = value
                quantities[bound, resource] = quantity
        for resource in ('cpu', 'memory'):
            if quantities['requests', resource] > quantities['limits', resource]:
                raise ValueError(
                    f'CALRISSIAN_DASK_CLEANUP_{resource.upper()}_REQUEST must not exceed '
                    f'CALRISSIAN_DASK_CLEANUP_{resource.upper()}_LIMIT')
        return resources

    def container_environment(self, environment=None):
        env_entries = []
        source_environment = self.environment if environment is None else environment
        for name, value in sorted(source_environment.items()):
            env_entries.append({'name': name, 'value': value})
        
        env_entries.append({'name': 'DASK_GATEWAY_WORKER_CORES', 'value': str(self.dask_requirement.get("workerCores"))})
        env_entries.append({'name': 'DASK_GATEWAY_WORKER_MEMORY', 'value': str(self.dask_requirement.get("workerMemory"))})
        env_entries.append({'name': 'DASK_GATEWAY_WORKER_CORES_LIMIT', 'value': str(self.dask_requirement.get("workerCoresLimit"))})
        env_entries.append({'name': 'DASK_GATEWAY_CLUSTER_MAX_CORES', 'value': str(self.dask_requirement.get("clusterMaxCores"))})
        env_entries.append({'name': 'DASK_GATEWAY_CLUSTER_MAX_RAM', 'value': str(self.dask_requirement.get("clusterMaxMemory"))})
        env_entries.append({'name': 'DASK_GATEWAY_URL', 'value': str(self.dask_gateway_url)})
        env_entries.append({'name': 'DASK_GATEWAY_IMAGE', 'value': str(self.container_image)})
        env_entries.append({'name': 'DASK_CLUSTER_NAME_PATH', 'value': '/shared/dask_cluster_name.txt'})

        return env_entries


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
                'name': self.init_container_name() + '-prepare',
                'image':  os.environ.get(INIT_IMAGE_ENV_VARIABLE, DEFAULT_INIT_IMAGE),
                'command': ['/bin/sh', '-c', ' '.join(command_list)],
                'workingDir': self.container_workingdir(),
                'volumeMounts': self.volume_mounts,
            })

        init_dask_cluster = {
            'name': self.init_container_name(),
            'image': str(self.container_image),
            'env': self.container_environment(self.helper_environment),
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
                        'env': self.container_environment(self.helper_environment),
                        'resources': self.sidecar_container_resources(),
                        'volumeMounts': self.volume_mounts,
                        'workingDir': self.container_workingdir(),
                        }
                ],
                'restartPolicy': 'Never',
                'automountServiceAccountToken': self.automount_service_account_token,
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

        return apply_gpu_tolerations(
            self.apply_container_security_context(spec), self.gpu_tolerations)
    

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
        self.add_kubernetes_volumes(self.pathmapper, runtime, runtimeContext, any_path_okay=True)

        if self.generatemapper is not None:
            # Seems to be true if docker is a hard requirement
            # This evaluates to true if docker_is_required is true
            # Used only for generatemapper add volumes
            any_path_okay = self.builder.get_requirement("DockerRequirement")[1] or False
            self.add_kubernetes_volumes(self.generatemapper, runtime, runtimeContext, any_path_okay)


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
        

        resolved_environment = self.resolve_secrets(self.environment, runtimeContext.secret_store)
        helper_environment = self.environment_without_secrets(self.environment, runtimeContext.secret_store)
        builder_options = {}
        if helper_environment != resolved_environment:
            builder_options['helper_environment'] = helper_environment

        k8s_builder = KubernetesDaskPodBuilder(
            self.get_dask_gateway_url(runtimeContext),
            controller_cm_exists,
            self.name,
            self.builder,
            self._get_container_image(),
            resolved_environment,
            self.volume_builder.volume_mounts,
            self.volume_builder.volumes,
            quoted_arg_list(self.resolve_secrets(self.command_line, runtimeContext.secret_store)),
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
            self.owner_reference,
            **builder_options,
        )

        built = k8s_builder.build()
        self.log_pod_diagnostic(built, runtimeContext.secret_store)
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
        
        try:
            pod = self.create_kubernetes_runtime(runtimeContext) # analogous to create_runtime()
            self.client.set_secret_store(runtimeContext.secret_store)
            self.execute_kubernetes_pod(pod) # analogous to _execute()
            completion_result = self.wait_for_kubernetes_pod(cm_name = self.dask_cm_name)
            if completion_result.exit_code != 0:
                log_main.error(f"ERROR the command below failed in pod {get_pod_name(pod)}:")
                safe_command = self.redact_secrets(get_pod_command(pod), runtimeContext.secret_store)
                log_main.error("\t" + " ".join(safe_command))
            self.finish(completion_result, runtimeContext)
        finally:
            self.cleanup_secret_staged_files()
            self.client.set_secret_store(None)


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

    @staticmethod
    def format_log_entry(pod_name, log_entry):
        return {"timestamp": f"{datetime.utcnow().isoformat()}Z", "pod": pod_name, "entry": log_entry}

    @retry_exponential_if_exception_type((ApiException, HTTPError,), log)
    def follow_logs(self, status):
        pod_name = self.pod.metadata.name
        log.info('[%s] follow_logs start', pod_name)

        kwargs = {
            "follow": True,
            "_preload_content": False,
        }
        if status is not None:
            kwargs["container"] = status.name

        resp = self.core_api_instance.read_namespaced_pod_log(
            name=pod_name,
            namespace=self.namespace,
            timestamps=False,
            tail_lines=200,
            **kwargs
        )

        try:
            for raw in resp.stream():
                if self._log_stop.is_set():
                    break

                if status is not None and not status.state.running:
                    break

                line = raw.decode("utf-8", errors="ignore").rstrip()
                log.debug('[%s] %s', pod_name, line)
                self.tool_log.append(self.format_log_entry(pod_name, line))
        finally:
            try:
                resp.close()
            except Exception:
                pass
            log.info('[%s] follow_logs end', pod_name)

    def _ensure_log_thread_started(self, status):
        # Main and cleanup containers need independent followers. A single
        # follower would discard cleanup logs required by the lifecycle checks.
        if not hasattr(self, '_container_log_threads'):
            self._container_log_threads = {}
        if status.name in self._container_log_threads:
            return
        self._log_stop.clear()
        thread = threading.Thread(target=self.follow_logs,
                                  name=f"pod-log-follower-{status.name}",
                                  args=(status,), daemon=True)
        self._container_log_threads[status.name] = thread
        thread.start()

    def _stop_log_thread(self):
        self._log_stop.set()
        for thread in getattr(self, '_container_log_threads', {}).values():
            if thread.is_alive():
                thread.join(timeout=5)
        self._container_log_threads = {}

    @retry_exponential_if_exception_type((ApiException, HTTPError), log)
    def wait_for_completion(self, cm_name: str=None) -> CompletionResult:

        resource_version = None

        while True:
            try:
                w = watch.Watch()

                pod_list = self.core_api_instance.list_namespaced_pod(
                    self.namespace, 
                    field_selector=self._get_pod_field_selector(),
                    limit = 1
                )

                if pod_list.items:
                    resource_version = pod_list.metadata.resource_version
                
                events_seen = False
                
                for event in w.stream(
                    func=self.core_api_instance.list_namespaced_pod,
                    namespace=self.namespace,
                    field_selector=self._get_pod_field_selector(),
                    resource_version=resource_version,
                    timeout_seconds=30
                ):
                    events_seen = True
                    pod = event['object']
                    resource_version = pod.metadata.resource_version

                    init_status = self.get_list_or_none(pod.status.init_container_statuses)
                    log.info("pod %s uid=%s status=%s", pod.metadata.name, pod.metadata.uid, init_status)
                    if init_status and self.state_is_terminated(init_status[0].state):
                        init_status_code = init_status[0].state.terminated.exit_code
                        if init_status_code is not None and init_status_code != 0:
                            with DaskPodMonitor() as monitor:
                                self.delete_pod_name(pod.metadata.name)
                                self.delete_configmap_name(cm_name=cm_name)
                                monitor.remove(pod)
                            self._clear_pod()
                            w.stop()

                    statuses = self.get_list_or_none(pod.status.container_statuses)
                    main_status = self.get_container_by_name(statuses, 'main-container')
                    # Wait for cleanup too, but never use its exit status for CWL.
                    all_terminated = bool(statuses) and all(
                        self.state_is_terminated(status.state) for status in statuses)
                    if not all_terminated:
                        statuses = self.get_list_or_none(pod.status.container_statuses)
                        if statuses is None:
                            continue
                        for status in statuses:
                            log.info("pod %s uid=%s status=%s", pod.metadata.name, pod.metadata.uid, status)

                            if status is None:
                                continue

                            if self.state_is_waiting(status.state):
                                continue

                            if self.state_is_running(status.state):
                                self._ensure_log_thread_started(status)
                                continue

                            if self.state_is_terminated(status.state):
                                continue

                            w.stop()
                            raise CalrissianJobException("Unexpected pod container status", status)

                        continue
                    
                    if main_status is None:
                        raise CalrissianJobException("Container 'main-container' not found in pod status", pod)
                    if self.state_is_terminated(main_status.state):
                        log.info("Handling terminated pod %s uid=%s", pod.metadata.name, pod.metadata.uid)

                        self._stop_log_thread()

                        container = self.get_container_by_name(pod.spec.containers, 'main-container')
                        if container is None:
                            raise CalrissianJobException("Container 'main-container' not found in pod spec", pod)
                        
                        node_selectors = self._get_pod_node_selector()
                        self._handle_completion(main_status.state, container, node_selectors)

                        if self.should_delete_pod():
                            with DaskPodMonitor() as monitor:
                                self.delete_pod_name(pod.metadata.name)
                                self.delete_configmap_name(cm_name=cm_name)
                                monitor.remove(pod)
                        
                        self._clear_pod()
                        w.stop()
                        

                        if self.completion_result is None:
                            raise IncompleteStatusException
                        return self.completion_result

                if not events_seen:
                    continue
            
            except ApiException as e:
                if e.status == 410:
                    log.warning("Watch expired (410 Gone). Re-listing and restarting watch.")
                    resource_version = None
                    continue
                raise
    
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
