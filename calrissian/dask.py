import os
import logging
from typing import Optional
import yaml

from cwltool.utils import CWLObjectType

from calrissian.job import (
    CalrissianCommandLineJob,
    KubernetesPodBuilder,
)
from calrissian.job import (
    quoted_arg_list
)
from calrissian.job import (
    DEFAULT_INIT_IMAGE,
    INIT_IMAGE_ENV_VARIABLE
)

log = logging.getLogger("calrissian.dask")
log_main = logging.getLogger("calrissian.main")

def dask_req_validate(requirement: Optional[CWLObjectType]) -> bool:
    if requirement is None:
        return False
    
    required_keys = ["workerCores", 
                     "workerCoresLimit", 
                     "workerMemory", 
                     "clustermaxCore", 
                     "clusterMaxMemory", 
                     "class"]
    
    return set(requirement.keys()) == set(required_keys)


class KubernetesDaskPodBuilder(KubernetesPodBuilder):

    def __init__(self,
                 name,
                 container_image,
                 environment,
                 volume_mounts,
                 volumes,
                 command_line,
                 stdout,
                 stderr,
                 stdin,
                 resources,
                 labels,
                 nodeselectors,
                 security_context,
                 serviceaccount,
                 gateway_url,
                 requirements=None,
                 hints=None):
        self.name = name
        self.container_image = container_image
        self.environment = environment
        self.volume_mounts = volume_mounts
        self.volumes = volumes
        self.command_line = command_line
        self.stdout = stdout
        self.stderr = stderr
        self.stdin = stdin
        self.resources = resources
        self.labels = labels
        self.nodeselectors = nodeselectors
        self.security_context = security_context
        self.serviceaccount = serviceaccount
        self.gateway_url = gateway_url
        self.requirements = {} if requirements is None else requirements
        self.hints = [] if hints is None else hints
    
    def container_command(self):
        super_command = super().container_command()
        super_args = super().container_args()
        
        shell_script = f"""\
set -e;
trap 'touch /shared/completed' EXIT;
export DASK_CLUSTER=$(cat /shared/dask_cluster_name.txt);
python -m app {super_args[0]}
"""
        super_command.append(shell_script)
        return super_command


    def container_environment(self):
        environment = []
        for name, value in sorted(self.environment.items()):
            environment.append({'name': name, 'value': value})
        
        environment.append({'name': 'PYTHONPATH', 'value': '/app'})

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
        
        dask_requirement = next((elem for elem in self.requirements if elem['class'] == 'https://calrissian-cwl.github.io/schema#DaskGatewayRequirement'), None)

        init_dask_command = [
            'python',
            '/app/init-dask.py',
            '--target',
            '/shared/dask_cluster_name.txt',
            '--gateway-url',
            self.gateway_url,
            '--image',
            str(self.container_image),
            '--worker-cores',
            str(dask_requirement["workerCores"]),
            '--worker-memory',
            str(dask_requirement["workerMemory"]),
            '--worker-cores-limit',
            str(dask_requirement["workerCoresLimit"]),
            '--max-cores',
            str(dask_requirement["clustermaxCore"]),
            '--max-ram',
            str(dask_requirement["clusterMaxMemory"])
        ]

        init_dask_cluster = {
            'name': self.init_container_name(),
            'image': str(self.container_image),
            'env': [{'name': 'PYTHONPATH', 'value': '/app'}],
            'command': init_dask_command,
            'workingDir': self.container_workingdir(),
            'volumeMounts': self.volume_mounts
        }

        containers.append(init_dask_cluster)

        return containers


    def build(self):

        sidecar_command = [
            'python',
            '/app/dispose-dask.py',
            '--source',
            '/shared/dask_cluster_name.txt',
            '--gateway-url',
            self.gateway_url,
            '--signal',
            '/shared/completed'  
        ]

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
                        'env': self.container_environment(),
                        'resources': self.container_resources(),
                        'volumeMounts': self.volume_mounts,
                        'workingDir': self.container_workingdir(),
                    },
                    {
                        'name': 'sidecar-container',
                        'image': str(self.container_image),
                        'command': sidecar_command,
                        'env': self.container_environment(),
                        'resources': self.container_resources(),
                        'volumeMounts': self.volume_mounts,
                        'workingDir': self.container_workingdir(),
                        }
                ],
                'restartPolicy': 'Never',
                'volumes': self.volumes,
                'securityContext': self.security_context,
                'nodeSelector': self.pod_nodeselectors()
            }
        }
        
        if ( self.serviceaccount ):
            spec['spec']['serviceAccountName'] = self.serviceaccount
        
        return spec
    

class CalrissianCommandLineDaskJob(CalrissianCommandLineJob):

    container_shared_dir = '/shared'
    daskGateway_config_dir = '/etc/dask'
    daskGateway_cm_name = 'dask-gateway-cm'
    daskGateway_cm = 'dask-gateway-cm'

    def __init__(self, *args, **kwargs):
        super(CalrissianCommandLineJob, self).__init__(*args, **kwargs)
        super(CalrissianCommandLineDaskJob, self).__init__(*args, **kwargs)

    def wait_for_kubernetes_pod(self):
        return self.client.wait_for_dask_completion()

    def get_dask_gateway_url(self, runtimeContext):
        return runtimeContext.gateway_url
    
    def _add_configmap_volume_and_binding(self, name, cm_name, target):
        self.volume_builder.add_configmap_volume(name, cm_name)
        self.volume_builder.add_configmap_volume_binding(cm_name, target)

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


        # self.client.create_dask_gateway_cofig_map(gateway_url=self.get_dask_gateway_url(runtimeContext))

        # emptyDir volume at /shared for sharing the Dask cluster name between containers
        self._add_emptydir_volume_and_binding('shared-data', self.container_shared_dir)
        

        # Need this ConfigMap to simplify configuration by providing defaults, 
        # as explained here: https://gateway.dask.org/configuration-user.html
        self._add_configmap_volume_and_binding(
            name=self.daskGateway_cm,
            cm_name=self.daskGateway_cm_name,
            target=self.daskGateway_config_dir)
        

        k8s_builder = KubernetesDaskPodBuilder(
            self.name,
            self._get_container_image(),
            self.environment,
            self.volume_builder.volume_mounts,
            self.volume_builder.volumes,
            self.quoted_command_line(),
            self.stdout,
            self.stderr,
            self.stdin,
            self.builder.resources,
            self.get_pod_labels(runtimeContext),
            self.get_pod_nodeselectors(runtimeContext),
            self.get_security_context(runtimeContext),
            self.get_pod_serviceaccount(runtimeContext),
            self.get_dask_gateway_url(runtimeContext),
            requirements=self.builder.requirements,
            hints=self.builder.hints,
        )

        built = k8s_builder.build()
        log.debug('{}\n{}{}\n'.format('-' * 80, yaml.dump(built), '-' * 80))
        # Report an error if anything was added to the runtime list
        if runtime:
            log.error('Runtime list is not empty. k8s does not use that, so you should see who put something there:\n{}'.format(' '.join(runtime)))
        return built
    
    def run(self, runtimeContext, tmpdir_lock=None):
        def get_pod_command(pod):
            if 'args' in pod['spec']['containers'][0].keys():
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
        completion_result = self.wait_for_kubernetes_pod()
        if completion_result.exit_code != 0:
            log_main.error(f"ERROR the command below failed in pod {get_pod_name(pod)}:")
            log_main.error("\t" + " ".join(get_pod_command(pod)))
        self.finish(completion_result, runtimeContext)