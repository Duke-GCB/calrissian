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

