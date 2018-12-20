from cwltool.job import CommandLineJob, relink_initialworkdir
from cwltool.pathmapper import ensure_writable
from cwltool.process import stage_files
from k8s import Client
import logging
import os
import shutil
import tempfile

log = logging.getLogger("seawall.job")

class KubernetesJobBuilder(object):

    def __init__(self, seawall_job, name):
        self.seawall_job = seawall_job
        self.name = name

    def job_name(self):
        return '{}-job'.format(self.name)

    def container_name(self):
        return '{}-container'.format(self.name)

    def container_image(self):
        """
        The docker image to use
        :return: Name of docker image from CWL DockerRequirement.
        """
        # Only looking at dockerPull here, since we are not implementing local image lookups
        docker_req, _ = self.seawall_job.get_requirement("DockerRequirement")
        return str(docker_req['dockerPull'])

    def container_volume_mounts(self):
        """
        Array of volume mounts
        :return:
        """
        mounts = []
        for index, volume in enumerate(self.seawall_job.volumes):
            mounts.append({
                'name': '{}-vol-{}'.format(self.name, index),
                'mountPath': volume[1]
            })

        # Also need an output volume
        mounts.append({
            'name': '{}-vol-outdir'.format(self.name),
            'mountPath': self.seawall_job.builder.outdir
        })
        return mounts

    def volumes(self):
        """
        Array of volumes to attach
        :return:
        """
        volumes = []
        for index, volume in enumerate(self.seawall_job.volumes):
            volumes.append({
                'name': '{}-vol-{}'.format(self.name, index),
                'hostPath': {
                    'path': volume[0]
                    # Leaving off type here since we won't be using hostPath long-term
                }
            })
        # And also add the outdir
        volumes.append({
            'name': '{}-vol-outdir'.format(self.name),
            'hostPath': {
                'path': self.seawall_job.outdir
            }
        })
        return volumes

    # To provide the CWL command-line to kubernetes, we must wrap it in 'sh -c <command string>'
    # Otherwise we can't do things like redirecting stdout.

    def container_command(self):
        return ['/bin/sh', '-c']

    def container_args(self):
        # TODO: Check shouldquote/shellescape/pipes.quote/etc
        # TODO: stdout/in/err may point to directory paths that don't exist yet
        # so add a container beforehand with a simple script that creates those
        # I think a k8s job can have multiple containers in sequence.
        job_command = self.seawall_job.command_line.copy()
        if self.seawall_job.stdout:
            job_command.extend(['>', self.seawall_job.stdout])
        if self.seawall_job.stderr:
            job_command.extend(['2>', self.seawall_job.stderr])
        if self.seawall_job.stdin:
            job_command.extend(['<', self.seawall_job.stdin])
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
        for name, value in self.seawall_job.environment.items():
            environment.append({'name': name, 'value': value})
        return environment

    def container_workingdir(self):
        """
        Return the working directory for this container
        :return:
        """
        #TODO: Handle DockerOutputDir, here if appropriate
        return self.seawall_job.environment['HOME']

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
                                'image': self.container_image(),
                                'command': self.container_command(),
                                'args': self.container_args(),
                                'env': self.container_environment(),
                                'volumeMounts': self.container_volume_mounts(),
                                'workingDir': self.container_workingdir(),
                             }
                        ],
                        'restartPolicy': 'Never',
                        'volumes': self.volumes()
                    }
                }
            }
        }


class SeawallCommandLineJob(CommandLineJob):

    def __init__(self, *args, **kwargs):
        super(SeawallCommandLineJob, self).__init__(*args, **kwargs)
        self.volumes = []
        self.client = Client(os.getenv('K8S_NAMESPACE', 'default'))

    def make_tmpdir(self):
        # Doing this because cwltool.job does it
        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)

    def populate_env_vars(self):
        # cwltool command-line job sets this to self.outdir, but reana uses self.builder.outdir
        # Update: it looks like self.builder.outdir is the inside-container variant, while self.outdir is the host path
        self.environment["HOME"] = self.builder.outdir
        self.environment["TMPDIR"] = self.tmpdir
        # Reana also sets some varaibles from os.environ, but those wouldn't make any sense here
        # Looks like leftovers from cwl's local executor

    def populate_volumes(self):
        pathmapper = self.pathmapper
        # This method copied from cwl_reana.py.
        # what does it do?

        host_outdir = self.outdir
        container_outdir = self.builder.outdir
        for src, vol in pathmapper.items():
            if not vol.staged:
                continue
            if vol.target.startswith(container_outdir + "/"):
                host_outdir_tgt = os.path.join(
                    host_outdir, vol.target[len(container_outdir) + 1:])
            else:
                host_outdir_tgt = None
            if vol.type in ("File", "Directory"):
                if not vol.resolved.startswith("_:"):
                    resolved = vol.resolved
                    if not os.path.exists(resolved):
                        resolved = "/".join(
                            vol.resolved.split("/")[:-1]) + "/" + \
                            vol.target.split("/")[-1]
                    self.volumes.append((resolved, vol.target))
            elif vol.type == "WritableFile":
                if self.inplace_update:
                    self.volumes.append((vol.resolved, vol.target))
                else:
                    shutil.copy(vol.resolved, host_outdir_tgt)
                    ensure_writable(host_outdir_tgt)
            if vol.type == "WritableDirectory":
                if vol.resolved.startswith("_:"):
                    if not os.path.exists(vol.target):
                        os.makedirs(vol.target, mode=0o0755)
                else:
                    if self.inplace_update:
                        pass
                    else:
                        shutil.copytree(vol.resolved, host_outdir_tgt)
                        ensure_writable(host_outdir_tgt)
            elif vol.type == "CreateFile":
                # This is the case where the file contents are literal in the job order
                # So this code can simply write that out to temporary space in the filesystem
                # but it doesn't make it into self.volumes list
                # Looks like the underlying assumption is that we can just write that out locally from the engine
                # and it goes to the directory that will be the working dir for the container
                if host_outdir_tgt:
                    with open(host_outdir_tgt, "wb") as f:
                        f.write(vol.resolved.encode("utf-8"))
                else:
                    fd, createtmp = tempfile.mkstemp(dir=self.tmpdir)
                    with os.fdopen(fd, "wb") as f:
                        f.write(vol.resolved.encode("utf-8"))

    def submit_kubernetes_job(self):
        k8s_builder = KubernetesJobBuilder(self, self.name)
        built = k8s_builder.build()
        import yaml
        log.debug('{}\n{}{}\n'.format('-' * 80, yaml.dump(built), '-' * 80))
        self.client.submit_job(built)

    def wait_for_kubernetes_job(self):
        self.client.wait()

    def finish(self):
        status = 'success'
        # collect_outputs (and collect_output) is definied in command_line_tool
        # It needs filesystem access to compute checksums and stuff
        # can that happen remotely or does it need to happen here?
        # I guess this needs to run in k8s too, so it might as well all have that assumption that it's per bespin-job
        outputs = self.collect_outputs(self.outdir)
        self.output_callback(outputs, status)

    def run(self, runtimeContext):
        self._setup(runtimeContext)
        self.make_tmpdir()
        self.populate_env_vars()
        # Removed stage_files call - cwltool docker doesn't use it
        self.populate_volumes()
        self.submit_kubernetes_job()
        self.wait_for_kubernetes_job()
        self.finish()
