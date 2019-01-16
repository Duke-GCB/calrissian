from cwltool.job import ContainerCommandLineJob
from cwltool.utils import DEFAULT_TMP_PREFIX
from calrissian.k8s import KubernetesClient
import logging
import os
import yaml
import shutil
import tempfile
import random
import string
from cwltool.pathmapper import ensure_writable, ensure_non_writable

log = logging.getLogger("calrissian.job")

class VolumeBuilderException(Exception):
    pass


def k8s_safe_name(name):
    """
    Kubernetes does not allow underscores
    DNS-1123 subdomain must consist of lower case alphanumeric characters, '-' or '.',
    and must start and end with an alphanumeric character (e.g. 'example.com', regex used
    for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*'),
    :param name:
    :return: a safe name
    """
    return name.lower().replace('_', '-')


def populate_demo_volume_builder_entries(volume_builder):
    volume_builder.add_persistent_volume_entry('/calrissian/input-data', 'calrissian-input-data')
    volume_builder.add_persistent_volume_entry('/calrissian/output-data', 'calrissian-output-data')
    volume_builder.add_persistent_volume_entry('/calrissian/tmptmp', 'calrissian-tmp')
    volume_builder.add_persistent_volume_entry('/calrissian/tmpout', 'calrissian-tmpout')


class KubernetesVolumeBuilder(object):

    def __init__(self):
        self.persistent_volume_entries = {}
        self.volume_mounts = []
        self.volumes = []

    def add_persistent_volume_entry(self, prefix, claim_name):
        entry = {
            'prefix': prefix,
            'volume': {
                'name': claim_name,
                'persistentVolumeClaim': {
                    'claimName': claim_name
                }
            }
        }
        self.persistent_volume_entries[prefix] = entry
        self.volumes.append(entry['volume'])

    def find_persistent_volume(self, source):
        """
        For a given source path, return the volume entry that contains it
        """
        for prefix, entry in self.persistent_volume_entries.items():
            if source.startswith(prefix):
                return entry
        return None

    @staticmethod
    def calculate_subpath(source, prefix):
        return source[len(prefix) + 1:]

    @staticmethod
    def random_tag(length=8):
        return ''.join(random.choices(string.ascii_lowercase, k=length))

    def add_volume_binding(self, source, target, writable):
        # Find the persistent volume claim where this goes
        pv = self.find_persistent_volume(source)
        if not pv:
            raise VolumeBuilderException('Could not find a persistent volume mounted for {}'.format(source))
        # Now build up the volumeMount entry for this container
        volume_mount = {
            'name': pv['volume']['name'],
            'mountPath': target,
            'subPath': self.calculate_subpath(source, pv['prefix']),
            'readOnly': not writable
        }
        self.volume_mounts.append(volume_mount)


class KubernetesPodBuilder(object):

    def __init__(self, name, container_image, environment, volume_mounts, volumes, command_line, stdout, stderr, stdin):
        self.name = name
        self.container_image = container_image
        self.environment = environment
        self.volume_mounts = volume_mounts
        self.volumes = volumes
        self.command_line = command_line
        self.stdout = stdout
        self.stderr = stderr
        self.stdin = stdin

    def pod_name(self):
        return k8s_safe_name('{}-pod'.format(self.name))

    def container_name(self):
        return k8s_safe_name('{}-container'.format(self.name))

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
        # and passed as an argument to sh -c. Otherwise we cannot redirect STDIN/OUT/ERR inside a kubernetes job
        # Join everything into a single string and then return a single args list
        return [' '.join(pod_command)]

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

    def build(self):
        return {
            'metadata': {
                'name': self.pod_name()
            },
            'apiVersion': 'v1',
            'kind':'Pod',
                'spec': {
                    'containers': [
                        {
                            'name': self.container_name(),
                            'image': self.container_image,
                            'command': self.container_command(),
                            'args': self.container_args(),
                            'env': self.container_environment(),
                            'volumeMounts': self.volume_mounts,
                            'workingDir': self.container_workingdir(),
                         }
                    ],
                    'restartPolicy': 'Never',
                    'volumes': self.volumes
            }
        }


# This now subclasses ContainerCommandLineJob, but only uses two of its methods:
# create_file_and_add_volume and add_volumes
class CalrissianCommandLineJob(ContainerCommandLineJob):

    def __init__(self, *args, **kwargs):
        super(CalrissianCommandLineJob, self).__init__(*args, **kwargs)
        self.client = KubernetesClient()
        volume_builder = KubernetesVolumeBuilder()
        populate_demo_volume_builder_entries(volume_builder)
        self.volume_builder = volume_builder

    def make_tmpdir(self):
        # Doing this because cwltool.job does it
        if not os.path.exists(self.tmpdir):
            log.debug('os.makedirs({})'.format(self.tmpdir))
            os.makedirs(self.tmpdir)

    def populate_env_vars(self):
        # cwltool DockerCommandLineJob always sets HOME to self.builder.outdir
        # https://github.com/common-workflow-language/cwltool/blob/1.0.20181201184214/cwltool/docker.py#L338
        self.environment["HOME"] = self.builder.outdir
        # cwltool DockerCommandLineJob always sets TMPDIR to /tmp
        # https://github.com/common-workflow-language/cwltool/blob/1.0.20181201184214/cwltool/docker.py#L333
        self.environment["TMPDIR"] = '/tmp'

    def wait_for_kubernetes_job(self):
        return self.client.wait_for_completion()

    def finish(self, exit_code):
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
        # collect_outputs (and collect_output) is definied in command_line_tool
        outputs = self.collect_outputs(self.outdir)
        self.output_callback(outputs, status)

    def _get_container_image(self):
        # we only use dockerPull here
        # Could possibly make an API call to kubernetes to check for the image there, but that's not important right now
        (docker_req, docker_is_req) = self.get_requirement("DockerRequirement")
        return str(docker_req["dockerPull"])

    def create_kubernetes_runtime(self, runtimeContext):
        # In cwltool, the runtime list starts as something like ['docker','run'] and these various builder methods
        # append to that list with docker (or singularity) options like volume mount paths
        # As we build up kubernetes, these aren't really used this way so we leave it empty
        runtime = []

        # Append volume for outdir
        self._add_volume_binding(os.path.realpath(self.outdir), self.builder.outdir, writable=True)
        # Append volume for tmp
        self._add_volume_binding(os.path.realpath(self.tmpdir), '/tmp', writable=True)

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
            self.command_line,
            self.stdout,
            self.stderr,
            self.stdin
        )
        built = k8s_builder.build()
        log.debug('{}\n{}{}\n'.format('-' * 80, yaml.dump(built), '-' * 80))
        # Report an error if anything was added to the runtime list
        if runtime:
            log.error('Runtime list is not empty. k8s does not use that, so you should see who put something there:\n{}'.format(' '.join(runtime)))
        return built

    def execute_kubernetes_job(self, k8s_job):
        self.client.submit_job(k8s_job)

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

    def run(self, runtimeContext):
        self.make_tmpdir()
        self.populate_env_vars()
        self._setup(runtimeContext)
        k8s_job = self.create_kubernetes_runtime(runtimeContext) # analogous to create_runtime()
        self.execute_kubernetes_job(k8s_job) # analogous to _execute()
        k8s_exit_code = self.wait_for_kubernetes_job()
        self.finish(k8s_exit_code)

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

    @staticmethod
    def append_volume(runtime, source, target, writable=False):
        raise NotImplementedError('append_volume')

