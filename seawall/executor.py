from cwltool.executors import MultithreadedJobExecutor
import logging

log = logging.getLogger("seawall.executor")


class SeawallExecutor(MultithreadedJobExecutor):

    def run_jobs(self,
                 process,           # type: Process
                 job_order_object,  # type: Dict[Text, Any]
                 logger,
                 runtime_context     # type: RuntimeContext
                ):
        return super(SeawallExecutor, self).run_jobs(process, job_order_object, logger, runtime_context)
