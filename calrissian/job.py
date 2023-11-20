from typing import Dict
from cwltool.job import ContainerCommandLineJob, needs_shell_quoting_re

# override cwltool.cuda.cuda_check
def _cuda_check(cuda_req, requestCount):
    return 1

import cwltool.job
cwltool.job.cuda_check = _cuda_check

from cwltool.utils import DEFAULT_TMP_PREFIX
from cwltool.errors import WorkflowException, UnsupportedRequirement
from calrissian.k8s import KubernetesClient, CompletionResult
from calrissian.report import Reporter, TimedResourceReport
from cwltool.builder import Builder
import logging
import os
import yaml
import shutil
import tempfile
import random
import string
import shellescape
import re
from cwltool.utils import visit_class, ensure_writable

log = logging.getLogger("calrissian.job")
log_main = logging.getLogger("calrissian.main")


K8S_UNSAFE_REGEX = re.compile('[^-a-z0-9]')

# Environment variable used to override image name used for initContainers
INIT_IMAGE_ENV_VARIABLE = 'CALRISSIAN_INIT_IMAGE'
DEFAULT_INIT_IMAGE = 'alpine:3.10'


class VolumeBuilderException(WorkflowException):
    pass


class CalrissianCommandLineJobException(WorkflowException):
    pass


def k8s_safe_name(name):
    """
    Kubernetes does not allow underscores, and in some names/labels no '.'
    DNS-1123 label must consist of lower case alphanumeric characters or '-',
    and must start and end with an alphanumeric character
    regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?'

    :param name:
    :return: a safe name
    """
    # Ensure lowercase and replace unsafe characters with '-'
    return K8S_UNSAFE_REGEX.sub('-', name.lower())


def random_tag(length=8):
    return ''.join(random.choices(string.ascii_lowercase, k=length))


def read_yaml(filename):
    with open(filename) as f:
        return yaml.safe_load(f)


def quoted_arg_list(arg_list):
    shouldquote = needs_shell_quoting_re.search
    return [shellescape.quote(arg) if shouldquote(arg) else arg for arg in arg_list]


def total_size(outputs):
    """
    Recursively walk through an output dictionary object, totaling
    up file size from each dictionary where 'class' == 'File'
    :param outputs: output dictionary from a CWL job
    :return: Sum of all 'size' field values found
    """
    files = []
    visit_class(outputs, ("File",), files.append)
    # Per https://www.commonwl.org/v1.0/CommandLineTool.html#File
    # size is optional in the class, so default to 0 if not found
    return sum([f.get('size', 0) for f in files])


class KubernetesPodVolumeInspector(object):
    def __init__(self, pod):
        self.pod = pod

    def get_persistent_volumes_dict(self):
        persistent_volumes = {}
        for pod_volume in self.pod.spec.volumes:
            if pod_volume.persistent_volume_claim:
                claim_name = pod_volume.persistent_volume_claim.claim_name
                read_only = pod_volume.persistent_volume_claim.read_only or False
                persistent_volumes[pod_volume.name] = (claim_name, read_only)
        return persistent_volumes

    def get_first_container(self):
        return self.pod.spec.containers[0]

    def get_mounted_persistent_volumes(self):
        mounted_persistent_volumes = []
        persistent_volumes = self.get_persistent_volumes_dict()
        for volume_mount in self.get_first_container().volume_mounts:
            volume = persistent_volumes.get(volume_mount.name)
            if volume:
                claim_name, read_only = volume
                mounted_persistent_volumes.append(
                    (volume_mount.mount_path, volume_mount.sub_path, claim_name, read_only))
        return mounted_persistent_volumes


class KubernetesVolumeBuilder(object):

    def __init__(self):
        self.persistent_volume_entries = {}
        self.emptydir_volume_names = []
        self.volume_mounts = []
        self.volumes = []

    def add_persistent_volume_entries_from_pod(self, pod):
        """
        Add a mounted persistent volume claim for each mounted PVC in the specified pod
        :param pod: V1Pod
        """
        inspector = KubernetesPodVolumeInspector(pod)
        for mount_path, sub_path, claim_name, read_only in inspector.get_mounted_persistent_volumes():
            self.add_persistent_volume_entry(mount_path, sub_path, claim_name, read_only)

    def add_persistent_volume_entry(self, prefix, sub_path, claim_name, read_only):
        entry = {
            'prefix': prefix,
            'subPath': sub_path,
            'volume': {
                'name': claim_name,
                'persistentVolumeClaim': {
                    'claimName': claim_name,
                    'readOnly': read_only
                }
            }
        }
        self.persistent_volume_entries[prefix] = entry
        self.volumes.append(entry['volume'])

    def add_emptydir_volume(self, name):
        volume = {
            'name': name,
            'emptyDir': {},
        }
        self.emptydir_volume_names.append(name)
        self.volumes.append(volume)

    def find_persistent_volume(self, source):
        """
        For a given source path, return the volume entry that contains it
        """
        for prefix, entry in self.persistent_volume_entries.items():
            if source.startswith(prefix):
                return entry
        return None

    @staticmethod
    def calculate_subpath(source, prefix, parent_sub_path):
        slashed_prefix = os.path.join(prefix, '')  # add '/' to end of prefix if it is not there already
        source_without_prefix = source[len(slashed_prefix):]
        if parent_sub_path:
            return '{}/{}'.format(parent_sub_path, source_without_prefix)
        else:
            return source_without_prefix

    def add_volume_binding(self, source, target, writable):
        # Find the persistent volume claim where this goes
        pv = self.find_persistent_volume(source)
        if not pv:
            raise VolumeBuilderException('Could not find a persistent volume mounted for {}'.format(source))
        # Now build up the volumeMount entry for this container
        volume_mount = {
            'name': pv['volume']['name'],
            'mountPath': target,
            'subPath': self.calculate_subpath(source, pv['prefix'], pv['subPath']),
            'readOnly': not writable
        }
        self.volume_mounts.append(volume_mount)

    def add_emptydir_volume_binding(self, name, target):
        if not name in self.emptydir_volume_names:
            # fail if the name is not registered
            raise VolumeBuilderException('Could not find an emptyDir volume named {}'.format(name))
        volume_mount = {
            'name': name,
            'mountPath': target
        }
        self.volume_mounts.append(volume_mount)


class KubernetesPodBuilder(object):

    def __init__(self, name, container_image, environment, volume_mounts, volumes, command_line, stdout, stderr, stdin, resources, labels, nodeselectors, security_context, serviceaccount, requirements=None, hints=None):
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
        self.requirements = {} if requirements is None else requirements
        self.hints = [] if hints is None else hints

    def pod_name(self):
        tag = random_tag()
        return k8s_safe_name('{}-pod-{}'.format(self.name, tag))

    def container_name(self):
        return k8s_safe_name('{}-container'.format(self.name))

    def init_container_name(self):
        return k8s_safe_name('{}-init'.format(self.name))

    # To provide the CWL command-line to kubernetes, we must wrap it in 'sh -c <command string>'
    # Otherwise we can't do things like redirecting stdout.

    def container_command(self):
        return ['/bin/sh', '-c']

    def container_args(self):
        pod_command = self.command_line.copy()
        if self.stdout:
            pod_command.extend(['>', self.stdout])
        if self.stderr:
            pod_command.extend(['2>', self.stderr])
        if self.stdin:
            pod_command.extend(['<', self.stdin])
        # pod_command is a list of strings. Needs to be turned into a single string
        # and passed as an argument to sh -c. Otherwise we cannot redirect STDIN/OUT/ERR inside a kubernetes container
        # Join everything into a single string and then return a single args list
        return [' '.join(pod_command)]

    # If redirecting to stdout or stderr, we may need to make intermediate directories first
    # This can only happen inside the pod, so we use an initContainer to `mkdir -p` for any directories
    # that need to exist.
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
        return containers

    def container_environment(self):
        """
        Build the environment line for the kubernetes yaml
        :return: array of env variables to set
        """
        environment = []
        for name, value in sorted(self.environment.items()):
            environment.append({'name': name, 'value': value})
        return environment

    def container_workingdir(self):
        """
        Return the working directory for this container
        :return:
        """
        return self.environment['HOME']

    # Conversions from CWL Runtime variables https://www.commonwl.org/v1.0/CommandLineTool.html#Runtime_environment
    # to Kubernetes Resource requests/limits
    # https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#resource-requests-and-limits-of-pod-and-container
    #
    # runtime_context.select_resources returns a dictionary with keys 'cores','mem','tmpdirSize','outdirSize'

    @staticmethod
    def resource_type(cwl_field):
        if cwl_field == 'cores':
            return 'cpu'
        elif cwl_field == 'ram':
            return 'memory'
        else:
            return None

    @staticmethod
    def resource_value(kind, cwl_value):
        if kind == 'cpu':
            return '{}'.format(cwl_value)
        elif kind == 'memory':
            return '{}Mi'.format(cwl_value)
        else:
            return None

    def container_resources(self):
        log.debug(f'Building resources spec from {self.resources}')
        container_resources = {}
        for cwl_field, cwl_value in self.resources.items():
            resource_bound = 'requests'
            resource_type = self.resource_type(cwl_field)
            resource_value = self.resource_value(resource_type, cwl_value)
            if resource_type and resource_value:
                if not container_resources.get(resource_bound):
                    container_resources[resource_bound] = {}
                container_resources[resource_bound][resource_type] = resource_value

        # Add CUDA requirements from CWL
        for requirement in self.requirements:
            if requirement["class"] in ['cwltool:CUDARequirement', 'http://commonwl.org/cwltool#CUDARequirement']:
                log.debug('Adding CUDARequirement resources spec')

                resource_bound = 'requests'
                container_resources[resource_bound]['nvidia.com/gpu'] = str(requirement["cudaDeviceCountMin"])
                if "limits" in container_resources:
                    resource_bound = 'limits'
                    container_resources[resource_bound]['nvidia.com/gpu'] = str(requirement["cudaDeviceCountMax"])
                else:
                    container_resources['limits'] = {'nvidia.com/gpu': str(requirement["cudaDeviceCountMax"])}

        return container_resources

    def pod_labels(self):
        """
        Submitted labels must be strings
        :return:
        """
        return {str(k): str(v) for k, v in self.labels.items()}
    
    def pod_nodeselectors(self):
        """
        Submitted node selectors must be strings
        :return:
        """
        return {str(k): str(v) for k, v in self.nodeselectors.items()}

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
                        'name': self.container_name(),
                        'image': self.container_image,
                        'command': self.container_command(),
                        'args': self.container_args(),
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


# This now subclasses ContainerCommandLineJob, but only uses two of its methods:
# create_file_and_add_volume and add_volumes
class CalrissianCommandLineJob(ContainerCommandLineJob):

    container_tmpdir = '/tmp'

    def __init__(self, *args, **kwargs):
        super(CalrissianCommandLineJob, self).__init__(*args, **kwargs)
        self.client = KubernetesClient()
        volume_builder = KubernetesVolumeBuilder()
        volume_builder.add_persistent_volume_entries_from_pod(self.client.get_current_pod())
        self.volume_builder = volume_builder
            
    def make_tmpdir(self):
        # Doing this because cwltool.job does it
        if not os.path.exists(self.tmpdir):
            log.debug('os.makedirs({})'.format(self.tmpdir))
            os.makedirs(self.tmpdir)

    def populate_env_vars(self, runtimeContext):
        # cwltool DockerCommandLineJob always sets HOME to self.builder.outdir
        # https://github.com/common-workflow-language/cwltool/blob/1.0.20181201184214/cwltool/docker.py#L338
        self.environment["HOME"] = self.builder.outdir
        # cwltool DockerCommandLineJob always sets TMPDIR to /tmp
        # https://github.com/common-workflow-language/cwltool/blob/1.0.20181201184214/cwltool/docker.py#L333
        self.environment["TMPDIR"] = self.container_tmpdir
        # Extra environement variables set at runtime
        for k, v in self.get_pod_env_vars(runtimeContext).items():
            self.environment[str(k)] = str(v)

    def wait_for_kubernetes_pod(self):
        return self.client.wait_for_completion()

    def report(self, completion_result: CompletionResult, disk_bytes):
        """
        Convert the k8s-specific completion result into a report and submit it
        :param completion_result: calrissian.k8s.CompletionResult
        """
        report = TimedResourceReport.create(self.name, completion_result, disk_bytes)
        Reporter.add_report(report)

    def dump_tool_logs(self, name, completion_result: CompletionResult, runtime_context):
        """
        Dumps the tool logs
        """
        if not os.path.exists(runtime_context.tool_logs_basepath):
            log.debug(f'os.makedirs({runtime_context.tool_logs_basepath})')
            os.makedirs(runtime_context.tool_logs_basepath)
            
        log_filename = os.path.join(runtime_context.tool_logs_basepath, f"{name}.log")

        log.info(f"Writing pod {name} logs to {log_filename}")
                
        with open(log_filename, 'w') as f:
            for log_entry in completion_result.tool_log:
                f.write(f"{log_entry['timestamp']} - {log_entry['pod']} - {log_entry['entry']}\n")

    def finish(self, completion_result: CompletionResult, runtimeContext):
        exit_code = completion_result.exit_code
        if exit_code in self.successCodes:
            status = "success"
        elif exit_code in self.temporaryFailCodes:
            status = "temporaryFail"
        elif exit_code in self.permanentFailCodes:
            status = "permanentFail"
        elif exit_code == 0:
            status = "success"
        else:
            status = "permanentFail"
        
        # dump the tool logs
        if runtimeContext.tool_logs_basepath: 
            self.dump_tool_logs(self.name, completion_result, runtimeContext)

        # collect_outputs (and collect_output) is defined in command_line_tool
        outputs = self.collect_outputs(self.outdir, exit_code)

        disk_bytes = total_size(outputs)
        self.report(completion_result, disk_bytes)

        # Invoke the callback with a lock
        with runtimeContext.workflow_eval_lock:
            self.output_callback(outputs, status)

        # Cleanup our stagedir and tmp
        if self.stagedir is not None and os.path.exists(self.stagedir):
            log.debug('shutil.rmtree({}, {})'.format(self.stagedir, True))
            shutil.rmtree(self.stagedir, True)

        if runtimeContext.rm_tmpdir:
            log.debug('shutil.rmtree({}, {})'.format(self.tmpdir, True))
            shutil.rmtree(self.tmpdir, True)

    # Dictionary of supported features.
    # Not yet complete, only checks features of DockerRequirement
    supported_features = {
        'DockerRequirement': ['class', 'dockerPull'],
        'http://commonwl.org/cwltool#CUDARequirement': ['class', 'cudaDeviceCount', 'cudaDeviceCountMin', 'cudaDeviceCountMax'],
    }

    def check_requirements(self, runtimeContext):
        for feature in self.supported_features:
            requirement, is_required = self.get_requirement(feature)
            if requirement and is_required:
                for field in requirement:
                    if not field in self.supported_features[feature]:
                        raise UnsupportedRequirement('Error: feature {}.{} is not supported'.format(feature, field))

        

    def _get_container_image(self):
        docker_requirement, _ = self.get_requirement('DockerRequirement')
        if docker_requirement:
            container_image = docker_requirement['dockerPull']
        else:
            # No dockerRequirement, use the default container
            container_image = self.builder.find_default_container()
        if not container_image:
            raise CalrissianCommandLineJobException('Unable to create Job - Please ensure tool has a DockerRequirement with dockerPull or specify a default_container')
        return container_image

    def quoted_command_line(self):
        return quoted_arg_list(self.command_line)

    def get_pod_labels(self, runtimeContext):
        if runtimeContext.pod_labels:
            return read_yaml(runtimeContext.pod_labels)
        else:
            return {}
    
    def get_pod_nodeselectors(self, runtimeContext):
        if runtimeContext.pod_nodeselectors:
            return read_yaml(runtimeContext.pod_nodeselectors)
        else:
            return {}

    def get_pod_serviceaccount(self, runtimeContext):
        return runtimeContext.pod_serviceaccount


    def get_security_context(self, runtimeContext):
        if not runtimeContext.no_match_user:
            return {
                'runAsUser': os.getuid(),
                'runAsGroup': os.getgid()
            }
        else:
            return {}

    def get_pod_env_vars(self, runtimeContext):
        if runtimeContext.pod_env_vars:
            return read_yaml(runtimeContext.pod_env_vars)
        else:
            return {}

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

        k8s_builder = KubernetesPodBuilder(
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
            self.builder.requirements,
            self.builder.hints
        )
        built = k8s_builder.build()
        log.debug('{}\n{}{}\n'.format('-' * 80, yaml.dump(built), '-' * 80))
        # Report an error if anything was added to the runtime list
        if runtime:
            log.error('Runtime list is not empty. k8s does not use that, so you should see who put something there:\n{}'.format(' '.join(runtime)))
        return built

    def execute_kubernetes_pod(self, pod):
        self.client.submit_pod(pod)

    def _add_emptydir_volume_and_binding(self, name, target):
        self.volume_builder.add_emptydir_volume(name)
        self.volume_builder.add_emptydir_volume_binding(name, target)

    def _add_volume_binding(self, source, target, writable=False):
        self.volume_builder.add_volume_binding(source, target, writable)

    # Below are concrete implementations of methods called by add_volumes
    # They are based on https://github.com/common-workflow-language/cwltool/blob/1.0.20181201184214/cwltool/docker.py
    # But the key difference is that docker is invoked via command-line, so the ones in docker.py append to
    # a runtime list. Here, we instead call self._add_volume_binding()

    def add_file_or_directory_volume(self,
                                     runtime,         # type: List[Text]
                                     volume,          # type: MapperEnt
                                     host_outdir_tgt  # type: Optional[Text]
                                     ):
        """Append volume a file/dir mapping to the runtime option list."""
        if not volume.resolved.startswith("_:"):
            self._add_volume_binding(volume.resolved, volume.target) # this one defaults to read_only

    def add_writable_file_volume(self,
                                 runtime,          # type: List[Text]
                                 volume,           # type: MapperEnt
                                 host_outdir_tgt,  # type: Optional[Text]
                                 tmpdir_prefix     # type: Text
                                 ):
        """Append a writable file mapping to the runtime option list."""
        if self.inplace_update:
            self._add_volume_binding(volume.resolved, volume.target, writable=True)
        else:
            if host_outdir_tgt:
                # shortcut, just copy to the output directory
                # which is already going to be mounted
                log.debug('shutil.copy({}, {})'.format(volume.resolved, host_outdir_tgt))
                shutil.copy(volume.resolved, host_outdir_tgt)
            else:
                log.debug('tempfile.mkdtemp(dir={})'.format(self.tmpdir))
                tmpdir = tempfile.mkdtemp(dir=self.tmpdir)
                file_copy = os.path.join(
                    tmpdir, os.path.basename(volume.resolved))
                log.debug('shutil.copy({}, {})'.format(volume.resolved, file_copy))
                shutil.copy(volume.resolved, file_copy)
                self._add_volume_binding(file_copy, volume.target, writable=True)
            ensure_writable(host_outdir_tgt or file_copy)

    def add_writable_directory_volume(self,
                                      runtime,          # type: List[Text]
                                      volume,           # type: MapperEnt
                                      host_outdir_tgt,  # type: Optional[Text]
                                      tmpdir_prefix     # type: Text
                                      ):
        """Append a writable directory mapping to the runtime option list."""
        if volume.resolved.startswith("_:"):
            # Synthetic directory that needs creating first
            if not host_outdir_tgt:
                log.debug('tempfile.mkdtemp(dir={})'.format(self.tmpdir))
                new_dir = os.path.join(
                    tempfile.mkdtemp(dir=self.tmpdir),
                    os.path.basename(volume.target))
                self._add_volume_binding(new_dir, volume.target, writable=True)
            elif not os.path.exists(host_outdir_tgt):
                log.debug('os.makedirs({}, 0o0755)'.format(host_outdir_tgt))
                os.makedirs(host_outdir_tgt, 0o0755)
        else:
            if self.inplace_update:
                self._add_volume_binding(volume.resolved, volume.target, writable=True)
            else:
                if not host_outdir_tgt:
                    log.debug('tempfile.mkdtemp(dir={})'.format(self.tmpdir))
                    tmpdir = tempfile.mkdtemp(dir=self.tmpdir)
                    new_dir = os.path.join(
                        tmpdir, os.path.basename(volume.resolved))
                    log.debug('shutil.copytree({}, {})'.format(volume.resolved, new_dir))
                    shutil.copytree(volume.resolved, new_dir)
                    self._add_volume_binding(new_dir, volume.target, writable=True)
                else:
                    log.debug('shutil.copytree({}, {})'.format(volume.resolved, host_outdir_tgt))
                    shutil.copytree(volume.resolved, host_outdir_tgt)
                ensure_writable(host_outdir_tgt or new_dir)

    def _required_env(self) -> Dict[str, str]:
        # spec currently says "HOME must be set to the designated output
        # directory." but spec might change to designated temp directory.
        # runtime.append("--env=HOME=/tmp")
        return {
            "TMPDIR": self.CONTAINER_TMPDIR,
            "HOME": self.builder.outdir,
        }

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

        self._setup(runtimeContext)
        # specific setup for Kubernetes
        self.setup_kubernetes(runtimeContext)
        pod = self.create_kubernetes_runtime(runtimeContext) # analogous to create_runtime()
        self.execute_kubernetes_pod(pod) # analogous to _execute()
        completion_result = self.wait_for_kubernetes_pod()
        if completion_result.exit_code != 0:
            log_main.error(f"ERROR the command below failed in pod {get_pod_name(pod)}:")
            log_main.error("\t" + " ".join(get_pod_command(pod)))
        self.finish(completion_result, runtimeContext)
    
    def setup_kubernetes(self, runtime_context):
        cuda_req, _ = self.get_requirement("http://commonwl.org/cwltool#CUDARequirement")

        if cuda_req:
            if runtime_context.max_gpus:
                # if --max-gpus is set, set cudaDeviceCount to 1 
                # to pass the cwltool job.py check
                self.builder.resources["cudaDeviceCount"] = max(cuda_req["cudaDeviceCountMin"], cuda_req["cudaDeviceCountMax"])  
                self.builder.resources["cudaDeviceCountMin"] = cuda_req["cudaDeviceCountMin"]
                self.builder.resources["cudaDeviceCountMax"] = cuda_req["cudaDeviceCountMax"]
            else:
                raise WorkflowException('Error: set --max-gpus to run CWL files with the CUDARequirement')

    # Below are concrete implementations of the remaining abstract methods in ContainerCommandLineJob
    # They are not implemented and not expected to be called, so they all raise NotImplementedError

    def get_from_requirements(self,
                              r,                                    # type: Dict[Text, Text]
                              pull_image,                           # type: bool
                              force_pull=False,                     # type: bool
                              tmp_outdir_prefix=DEFAULT_TMP_PREFIX  # type: Text
                              ):
        raise NotImplementedError('get_from_requirements')

    def create_runtime(self,
                       env,             # type: MutableMapping[Text, Text]
                       runtime_context  # type: RuntimeContext
                       ):
        # expected to return runtime list and cid string
        raise NotImplementedError('create_runtime')

    def append_volume(self, runtime, source, target, writable=False):
        """Add volume binding to the arguments list.
        This is called by the base class for file literals after they've been created.
        We already have a similar function, so we just call that.
        """
        self._add_volume_binding(source, target, writable)