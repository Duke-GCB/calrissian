from cwltool.command_line_tool import CommandLineTool
from cwltool.workflow import default_make_tool
from calrissian.job import CalrissianCommandLineJob
import logging

log = logging.getLogger("calrissian.tool")

class CalrissianCommandLineTool(CommandLineTool):

    def make_job_runner(self, runtimeContext):
        # TODO: inject Docker requirement
        # This should return a callable
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

