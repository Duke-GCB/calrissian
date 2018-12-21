from cwltool.job import ContainerCommandLineJob
from cwltool.utils import DEFAULT_TMP_PREFIX
from k8s import Client
import logging
import os
import yaml
import shutil
import tempfile
from cwltool.pathmapper import ensure_writable, ensure_non_writable

log = logging.getLogger("seawall.job")


class KubernetesJobBuilder(object):

    def __init__(self, name, container_image, environment, volumes, command_line, stdout, stderr, stdin):
        self.name = name
        self.container_image = container_image
        self.environment = environment
        self.volumes = volumes
        self.command_line = command_line
        self.stdout = stdout
        self.stderr = stderr
        self.stdin = stdin

    @staticmethod
    def k8s_safe_name(name):
        """
        Kubernetes does not allow underscores
        DNS-1123 subdomain must consist of lower case alphanumeric characters, '-' or '.',
        and must start and end with an alphanumeric character (e.g. 'example.com', regex used
        for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*'),
        :param name:
        :return: a safe name
        """
        return name.replace('_', '-')

    def job_name(self):
        return self.k8s_safe_name('{}-job'.format(self.name))

    def container_name(self):
        return self.k8s_safe_name('{}-container'.format(self.name))

    def set_container_image(self):
        """
        The docker image to use
        :return: Name of docker image from CWL DockerRequirement.
        """
        # Only looking at dockerPull here, since we are not implementing local image lookups
        docker_req, _ = self.seawall_job.get_requirement("DockerRequirement")
        return str(docker_req['dockerPull'])

    def _volume_name(self, volume, index):
        return self.k8s_safe_name('{}-{}-{}'.format(self.name, volume['note'], index))

    def container_volume_mounts(self):
        """
        Array of volume mounts
        :return:
        """
        mounts = []
        for index, volume in enumerate(self.volumes):
            mounts.append({
                'name': self._volume_name(volume, index),
                'mountPath': volume['target'],
                'readOnly': not volume['writable']
            })
        # TODO: can we make these read-only?
        return mounts

    def container_volumes(self):
        """
        Array of volumes to attach
        :return:
        """
        volumes = []
        for index, volume in enumerate(self.volumes):
            volumes.append({
                'name': self._volume_name(volume, index),
                'hostPath': {
                    'path': volume['source']
                    # Leaving off type here since we won't be using hostPath long-term
                }
            })
        return volumes

    # To provide the CWL command-line to kubernetes, we must wrap it in 'sh -c <command string>'
    # Otherwise we can't do things like redirecting stdout.

    def container_command(self):
        return ['/bin/sh', '-c']

    def container_args(self):
        # TODO: Add missing features (quoting, creating directories for stdout, secret_store)
        # Look to _execute in https://github.com/common-workflow-language/cwltool/blob/1.0.20181201184214/cwltool/job.py

        # so add a container beforehand with a simple script that creates those
        # I think a k8s job can have multiple containers in sequence.
        # And for symmetry, where should the result code checking be? here or in the client?
        job_command = self.command_line.copy()
        if self.stdout:
            job_command.extend(['>', self.stdout])
        if self.stderr:
            job_command.extend(['2>', self.stderr])
        if self.stdin:
            job_command.extend(['<', self.stdin])
        # job_command is a list of strings. Needs to be turned into a single string
        # and passed as an argument to sh -c. Otherwise we cannot redirect STDIN/OUT/ERR inside a kubernetes job
        # Join everything into a single string and then return a single args list
        return [' '.join(job_command)]

    def container_environment(self):
        """
        Build the environment line for the kubernetes yaml
        :return: array of env variables to set
        """
        environment = []
        for name, value in self.environment.items():
            environment.append({'name': name, 'value': value})
        return environment

    def container_workingdir(self):
        """
        Return the working directory for this container
        :return:
        """
        #TODO: Handle DockerOutputDir, here if appropriate
        return self.environment['HOME']

    def build(self):
        return {
            'metadata': {
                'name': self.job_name()
            },
            'apiVersion': 'batch/v1',
            'kind':'Job',
            'spec': {
                'template': {
                    'spec': {
                        'containers': [
                            {
                                'name': self.container_name(),
                                'image': self.container_image,
                                'command': self.container_command(),
                                'args': self.container_args(),
                                'env': self.container_environment(),
                                'volumeMounts': self.container_volume_mounts(),
                                'workingDir': self.container_workingdir(),
                             }
                        ],
                        'restartPolicy': 'Never',
                        'volumes': self.container_volumes()
                    }
                }
            }
        }


# This now subclasses ContainerCommandLineJob, but only uses two of its methods:
# create_file_and_add_volume and add_volumes
class SeawallCommandLineJob(ContainerCommandLineJob):

    def __init__(self, *args, **kwargs):
        super(SeawallCommandLineJob, self).__init__(*args, **kwargs)
        self.volumes = []
        self.client = Client(os.getenv('K8S_NAMESPACE', 'default'))

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
        self.client.wait()
        #TODO: check the results

    def finish(self):
        #TODO Check the results for real and clean-up
        status = 'success'
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
        self._add_volume_binding(os.path.realpath(self.outdir), self.builder.outdir, 'outdir', writable=True)
        # Append volume for tmp
        self._add_volume_binding(os.path.realpath(self.tmpdir), '/tmp', 'tmp', writable=True)

        # Call the ContainerCommandLineJob add_volumes method
        self.add_volumes(self.pathmapper,
                         runtime,
                         tmpdir_prefix=runtimeContext.tmpdir_prefix,
                         secret_store=runtimeContext.secret_store,
                         any_path_okay=True)

        if self.generatemapper is not None:
            # TODO: look at what any_path_okay is for
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

        # TODO: Determine if we can port --read-only, networkaccess, log-driver, --user
        # TODO: Provide the CPU/memory limits

        k8s_builder = KubernetesJobBuilder(
            self.name,
            self._get_container_image(),
            self.environment,
            self.volumes,
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

    def _add_volume_binding(self, source, target, note='vol', writable=False):
        # TODO: Consider if this should have a field for a File or a Directory, and promote to custom object
        self.volumes.append({'source':source, 'target':target, 'note':note, 'writable':writable})

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
        self.wait_for_kubernetes_job()
        self.finish()

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

