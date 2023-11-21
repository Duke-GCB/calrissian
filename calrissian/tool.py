from cwltool.command_line_tool import CommandLineTool
from cwltool.workflow import default_make_tool
from calrissian.job import CalrissianCommandLineJob
import logging

log = logging.getLogger("calrissian.tool")


class CalrissianCommandLineToolException(BaseException):
    pass


class CalrissianCommandLineTool(CommandLineTool):

    def make_job_runner(self, runtimeContext):
        """
        Construct a callable that can run a CommandLineTool
        :param runtimeContext: RuntimeContext object
        :return: a callable that runs the job
        """
        # Calrissian runs CommandLineTools exclusively in containers, so must always return a CalrissianCommandLineJob
        # If the tool definition does NOT have a DockerRequirement, we inject one here before returning the job_runner.
        #
        # Notes on why this is done here (as opposed to inside the job):
        #
        # 1. The job() method in the base CommandLineTool class (which this class inherits) checks for DockerRequirement
        #    before setting the job's outdir, tmpdir, and stagedir. When the requirement is not present, local dirs
        #    are used, which don't get mounted correctly to the container:
        # See https://github.com/common-workflow-language/cwltool/blob/a94d75178c24ce77b59403fb8276af9ad1998929/cwltool/command_line_tool.py#L506-L515
        #
        # 2. The make_job_runner() method in the base CommandLineTool injects a DockerRequirement when appropriate.
        #    Since we're overriding that method, we must inject the same requirement to be consistent with behavior in
        #    (1) above.
        # See https://github.com/common-workflow-language/cwltool/blob/a94d75178c24ce77b59403fb8276af9ad1998929/cwltool/command_line_tool.py#L243

        if not runtimeContext.use_container:
            raise CalrissianCommandLineToolException('Unable to create a CalrissianCommandLineTool - use_container is disabled')
        docker_requirement, _ = self.get_requirement('DockerRequirement')
        if not docker_requirement:
            # no docker requirement specified, inject one
            default_container = runtimeContext.find_default_container(self)
            if not default_container:
                raise CalrissianCommandLineToolException('Unable to create a CalrissianCommandLineTool - '
                                              'tool has no DockerRequirement and no default_container specified')
            self.requirements.insert(0, {
                'class': 'DockerRequirement',
                'dockerPull': default_container
            })
        
        return CalrissianCommandLineJob


def calrissian_make_tool(spec, loadingContext):
    """
    Construct a Process object from a CWL document loaded into spec
    :param spec:
    :param loadingContext:
    :return: For CommandLineTools, return our specialized subclass that can run on k8s.
    For other types of documents, return the CWL default_make_tool
    """
    if "class" in spec and spec["class"] == "CommandLineTool":
        return CalrissianCommandLineTool(spec, loadingContext)
    else:
        return default_make_tool(spec, loadingContext)

