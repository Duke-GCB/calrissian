from cwltool.command_line_tool import CommandLineTool
from cwltool.workflow import default_make_tool
from job import SeawallCommandLineJob
import logging

log = logging.getLogger("seawall.tool")

class SeawallCommandLineTool(CommandLineTool):

    def make_job_runner(self, runtimeContext):
        log.info('inside make_job_runner')
        # TODO: inject Docker requirement
        # This should return a callable
        return SeawallCommandLineJob


def seawall_make_tool(spec, loadingContext):
    """
    Construct a Process object from a CWL document loaded into spec
    :param spec:
    :param loadingContext:
    :return: For CommandLineTools, return our specialized subclass that can run on k8s.
    For other types of documents, return the CWL default_make_tool
    """
    log.info('inside seawall_make_tool')

    if "class" in spec and spec["class"] == "CommandLineTool":
        return SeawallCommandLineTool(spec, loadingContext)
    else:
        return default_make_tool(spec, loadingContext)

