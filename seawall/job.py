from cwltool.job import CommandLineJob
import logging

log = logging.getLogger("seawall.job")


class SeawallCommandLineJob(CommandLineJob):

    def run(self, runtimeContext):
        log.info('Inside job.run()')
        return super(SeawallCommandLineJob, self).run(self, runtimeContext)

