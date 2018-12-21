from cwltool.executors import MultithreadedJobExecutor
import logging

log = logging.getLogger("seawall.executor")


class SeawallExecutor(MultithreadedJobExecutor):

    # MultithreadedJobExecutor sets self.max_ram and self.max_cores to the local machine's resources
    # TODO: override init to set this

    def run_jobs(self,
                 process,           # type: Process
                 job_order_object,  # type: Dict[Text, Any]
                 logger,
                 runtime_context     # type: RuntimeContext
                ):
        return super(SeawallExecutor, self).run_jobs(process, job_order_object, logger, runtime_context)
