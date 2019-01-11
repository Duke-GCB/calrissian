from cwltool.executors import MultithreadedJobExecutor
import logging

log = logging.getLogger("calrissian.executor")


class CalrissianExecutor(MultithreadedJobExecutor):

    # MultithreadedJobExecutor sets self.max_ram and self.max_cores to the local machine's resources

    def run_jobs(self,
                 process,           # type: Process
                 job_order_object,  # type: Dict[Text, Any]
                 logger,
                 runtime_context     # type: RuntimeContext
                ):
        return super(CalrissianExecutor, self).run_jobs(process, job_order_object, logger, runtime_context)
