from cwltool.command_line_tool import CommandLineTool
from cwltool.workflow import default_make_tool
from calrissian.job import CalrissianCommandLineJob
import logging

log = logging.getLogger("calrissian.tool")

class CalrissianToolException(BaseException):
    pass

class CalrissianCommandLineTool(CommandLineTool):

    def make_job_runner(self, runtimeContext):
        """
        Construct a callable that can run a CommandLineTool
        :param runtimeContext: RuntimeContext object
        :return: a callable that runs the job
        """
        # This implementation runs CommandLineTools exclusively in containers
        # so we need to add a DockerRequirement if it's not present
        if not runtimeContext.use_container:
            raise CalrissianToolException('Unable to create a CalrissianCommandLineTool - use_container is disabled')
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

