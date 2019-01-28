from cwltool.executors import MultithreadedJobExecutor
import logging

log = logging.getLogger("calrissian.executor")


class CalrissianExecutor(MultithreadedJobExecutor):

    def __init__(self, max_ram, max_cores):  # type: () -> None
        """
        Initialize a Calrissian Executor
        :param max_ram: Maximum RAM to use in megabytes
        :param max_cores: Maximum number of CPU cores to use
        """
        # MultithreadedJobExecutor sets self.max_ram and self.max_cores to the local machine's resources using psutil.
        # We can simply override these values after init, and the executor will use our provided values
        super(CalrissianExecutor, self).__init__()
        self.max_ram = max_ram
        self.max_cores = max_cores
        log.debug('Initialized executor to allow {} MB RAM and {} CPU cores'.format(self.max_ram, self.max_cores))

    def run_job(self, *args, **kwargs):
        self.report_usage('run_job')
        return super(CalrissianExecutor, self).run_job(*args, **kwargs)

    def wait_for_next_completion(self, *args, **kwargs):
        self.report_usage('wait_for_next_completion')
        return super(CalrissianExecutor, self).wait_for_next_completion(*args, **kwargs)

    def run_jobs(self, *args, **kwargs):
        self.report_usage('run_jobs')
        return super(CalrissianExecutor, self).run_jobs(*args, **kwargs)

    def execute(self, *args, **kwargs):
        self.report_usage('execute')
        return super(CalrissianExecutor, self).execute(*args, **kwargs)

    def report_usage(self, caller=''):
        log.debug('{}: Allocated {}/{} CPU cores, {}/{} MB RAM'.format(caller, self.allocated_cores, self.max_cores,
                                                                   self.allocated_ram, self.max_ram))
