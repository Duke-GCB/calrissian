import functools
import threading
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED, ALL_COMPLETED
from queue import Queue

from cwltool.errors import WorkflowException
from cwltool.executors import JobExecutor
from schema_salad.validate import ValidationException


class DuplicateJobException(Exception):
    pass


class OversizedJobException(Exception):
    pass


class InconsistentResourcesException(Exception):
    pass


class IncompleteStatusException(Exception):
    pass


class Resources(object):
    """
    Class to encapsulate compute resources and provide arithmetic operations and comparisons
    """
    RAM = 'ram'
    CORES = 'cores'
    GPUS = 'gpus'

    def __init__(self, ram=0, cores=0, gpus=0):
        self.ram = ram
        self.cores = cores
        self.gpus = gpus

    def __sub__(self, other):
        ram = self.ram - other.ram
        cores = self.cores - other.cores
        gpus = self.gpus - other.gpus
        return Resources(ram, cores, gpus)

    def __add__(self, other):
        ram = self.ram + other.ram
        cores = self.cores + other.cores
        gpus = self.gpus + other.gpus
        return Resources(ram, cores, gpus)

    def __neg__(self):
        return Resources(-self.ram, -self.cores, -self.gpus)

    def __lt__(self, other):
        return self.ram < other.ram and self.cores < other.cores and self.gpus < other.gpus

    def __gt__(self, other):
        return self.ram > other.ram and self.cores > other.cores and self.gpus > other.gpus

    def __eq__(self, other):
        return self.ram == other.ram and self.cores == other.cores and self.gpus == other.gpus

    def __ge__(self, other):
        return self.ram >= other.ram and self.cores >= other.cores and self.gpus >= other.gpus

    def __le__(self, other):
        return self.ram <= other.ram and self.cores <= other.cores and self.gpus <= other.gpus

    def __str__(self):
        return '[ram: {}, cores: {}, gpus {}]'.format(self.ram, self.cores, self.gpus)

    def is_negative(self):
        return self.ram < 0 or self.cores < 0 or self.gpus < 0

    def exceeds(self, other):
        return self.ram > other.ram or self.cores > other.cores or self.gpus > other.gpus

    def to_dict(self):
        return { Resources.CORES: self.cores,
                 Resources.RAM: self.ram,
                 Resources.GPUS: self.gpus }

    @classmethod
    def from_dict(cls, d):
        return cls(d.get(cls.RAM, 0), d.get(cls.CORES, 0), d.get(cls.GPUS, 0))

    @classmethod
    def from_job(cls, job):
        if hasattr(job, 'builder'):
            return cls.from_dict(job.builder.resources)
        else:
            return Resources.EMPTY

    @classmethod
    def min(cls, rsc1, rsc2):
        return Resources(min(rsc1.ram, rsc2.ram), min(rsc1.cores, rsc2.cores), min(rsc1.gpus, rsc2.gpus))


Resources.EMPTY = Resources(0, 0, 0)


class JobResourceQueue(object):
    """
    Contains a dictionary of jobs, mapped to their resources.
    Provides an interface for getting a subset of jobs that fit within a resource limit
    """

    def __init__(self, priority=Resources.RAM, descending=False):
        """
        Create a JobResourceQueue
        :param priority: Resources.RAM or Resources.CORES - Used as a sort key when de-queuing jobs
        :param descending: boolean: When True, the jobs requesting the most resource will be de-queued first.
        """
        self.jobs = dict()
        self.priority = priority
        self.descending = descending

    def enqueue(self, job):
        """
        Add a job to the queue. Raises if job already exists in the queue
        :param job: A job to add to the queue
        """
        if job in self.jobs:
            raise DuplicateJobException('Job already exists')
        if job:
            self.jobs[job] = Resources.from_job(job)

    def is_empty(self):
        """
        Is the queue empty
        :return: True if the queue is empty, False otherwise
        """
        return len(self.jobs) == 0

    def sorted_jobs(self):
        """
        Produces a list of the jobs in the queue, ordered by self.priority (RAM or CORES) and ascending or descending
        :return:
        """
        return sorted(self.jobs.items(), key=lambda item: getattr(item[1], self.priority), reverse=self.descending)

    def dequeue(self, resource_limit):
        """
        Collects jobs from the sorted list that fit together within the specified resource limit.
        Removes (pop) collected jobs from the queue (pop).
        May return an empty dictionary if queue is empty or no jobs fit.imit
        :param resource_limit: A Resource object
        :return: Dictionary of {Job:Resources}
        """
        jobs = {}
        for job, resource in self.sorted_jobs():
            if resource_limit - resource >= Resources.EMPTY:
                jobs[job] = resource
                resource_limit = resource_limit - resource
        for job in jobs:
            self.jobs.pop(job)
        return jobs


class ThreadPoolJobExecutor(JobExecutor):
    """
    A cwltool JobExecutor subclass that uses concurrent.futures.ThreadPoolExecutor

    concurrent.futures was introduced in Python 3.2 and provides high-level interfaces (Executor, Future) for launching
    and managing asynchronous and parallel tasks. The ThreadPoolExecutor maintains a pool of reusable threads to execute
    tasks, reducing the overall number of threads that are created in the cwltool process.

    Relevant: https://github.com/common-workflow-language/cwltool/issues/888
    """

    def __init__(self, total_ram, total_cores, total_gpus=0, max_workers=None):
        """
        Initialize a ThreadPoolJobExecutor
        :param total_ram: RAM limit in megabytes for concurrent jobs
        :param total_cores: cpu core count limit for concurrent jobs
        :param max_workers: Number of worker threads to create. Set to None to use Python's default of 5xcpu count, which
        should be sufficient. Setting max_workers too low can cause deadlocks.

        See https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
        """
        super(ThreadPoolJobExecutor, self).__init__()
        self.max_workers = max_workers
        self.jrq = JobResourceQueue()
        self.exceptions = Queue()
        self.total_resources = Resources(total_ram, total_cores, total_gpus)
        self.available_resources = Resources(total_ram, total_cores, total_gpus) # start with entire pool available
        self.resources_lock = threading.Lock()

    def select_resources(self, request, runtime_context):
        """
        Naïve check for available cores cores and memory
        Checks if requested resources fit within the total allocation, raises WorkflowException if not.
        If fits, returns a dictionary of resources that satisfy the requested min/max

        :param request: dict of ramMin, coresMin, ramMax, coresMax
        :param runtime_context: RuntimeContext, unused
        :return: dict of selected resources
        """
        requested_min = Resources(request.get('ramMin'), request.get('coresMin'), request.get('cudaDeviceCountMin', 0))
        requested_max = Resources(request.get('ramMax'), request.get('coresMax'), request.get('cudaDeviceCountMax', 0))

        if requested_min.exceeds(self.total_resources):
            raise WorkflowException('Requested minimum resources {} exceed total available {}'.format(
                requested_min, self.total_resources
            ))

        result = Resources.min(requested_max, self.total_resources)
        return result.to_dict()

    def job_done_callback(self, rsc, logger, future):
        """
        Callback to run after a job is finished to restore reserved resources and check for exceptions.
        Expected to be called as part of the Future.add_done_callback(). The callback is invoked on a background
        thread. If the callback itself raises an Exception, that Exception is logged and ignored, so we instead
        queue exceptions and re-raise from the main thread.

        :param rsc: Resources used by the job to return to our available resources.
        :param logger: logger where messages shall be logged
        :param future: A concurrent.futures.Future representing the finished task. May be in cancelled or done states
        """

        # Always restore the resources.
        try:
            self.restore(rsc, logger)
        except Exception as ex:
            self.exceptions.put(ex)

        # if the future was cancelled, there is no more work to do. Bail out now because calling result() or
        # exception() on a cancelled future would raise a CancelledError in this scope.
        if future.cancelled():
            return

        # Check if the future raised an exception - may return None
        if future.exception():
            # The Queue is thread safe so we dont need a lock, even though we're running on a background thread
            self.exceptions.put(future.exception())

    def raise_if_exception_queued(self, futures, logger):
        """
        Method to run on the main thread that will raise a queued exception added by job_done_callback and
        cancel any outstanding futures
        :param futures: set of any futures that should be cancelled if we're about to raise an exception
        :param logger: logger where messages shall be logged
        """
        # Code translated from SingleJobExecutor.run_jobs
        # It raises WorkflowExceptions and ValidationException directly.
        # Other Exceptions are converted to WorkflowException
        if not self.exceptions.empty():
            # There's at least one exception, cancel all pending jobs
            # Note that cancel will only matter if there aren't enough available threads to start processing the job in
            # the first place. Once the function starts running it cannot be cancelled.
            logger.error('Found a queued exception, canceling outstanding futures')
            for f in futures:
                f.cancel()
            # Wait for outstanding futures to finish up so that cleanup can happen
            logger.error('Waiting for canceled futures to finish')
            wait(futures, return_when=ALL_COMPLETED)
            exceptions = []
            # Dequeue the exceptions into a list.
            while not self.exceptions.empty():
                exceptions.append(self.exceptions.get())
            if len(exceptions) == 1: # single exception queued
                try:
                    raise exceptions[0]
                except (WorkflowException, ValidationException):
                    raise
                except Exception as err:
                    logger.exception("Got workflow error")
                    raise WorkflowException(str(err)) from err
            else: # multiple exceptions were queued, raise multiple
                raise WorkflowException(str(exceptions)) from exceptions[0]

    def raise_if_oversized(self, job):
        """
        Raise an exception if a job does not fit within total_resources
        :param job: Job to check resources
        """
        rsc = Resources.from_job(job)
        if rsc.exceeds(self.total_resources):
            raise OversizedJobException('Job {} resources {} exceed total resources {}'.
                                        format(job, rsc, self.total_resources))

    def _account(self, rsc):
        with self.resources_lock:
            self.available_resources += rsc
            # Check if overallocated
            if self.available_resources.is_negative():
                raise InconsistentResourcesException('Available resources are negative: {}'.
                                                     format(self.available_resources))
            elif self.available_resources.exceeds(self.total_resources):
                raise InconsistentResourcesException('Available resources exceeds total. Available: {}, Total: {}'.
                                                     format(self.available_resources, self.total_resources))

    def allocate(self, rsc, logger):
        """
        Reserve resources from the total. Raises InconsistentResourcesException if available becomes negative
        :param rsc: A Resources object to reserve from the total.
        :param logger: logger where messages shall be logged
        """
        logger.debug('allocate {} from available {}'.format(rsc, self.available_resources))
        self._account(-rsc)

    def restore(self, rsc, logger):
        """
        Restore resources to the total. Raises InconsistentResourcesException if available becomes negative
        :param rsc: A Resources object to restore to the total
        :param logger: logger where messages shall be logged
        """
        logger.debug('restore {} to available {}'.format(rsc, self.available_resources))
        self._account(rsc)

    def start_queued_jobs(self, pool_executor, logger, runtime_context):
        """
        Pulls jobs off the queue in groups that fit in currently available resources, allocates resources, and submits
        jobs to the pool_executor as Futures. Attaches a callback to each future to clean up (e.g. check
        for execptions, restore allocated resources)
        :param pool_executor: concurrent.futures.Executor: where job callables shall be submitted
        :param logger: logger where messages shall be logged
        :param runtime_context: cwltool RuntimeContext: to provide to the job
        :return: set: futures that were submitted on this invocation
        """
        runnable_jobs = self.jrq.dequeue(self.available_resources)  # Removes jobs from the queue
        submitted_futures = set()
        for job, rsc in runnable_jobs.items():
            if runtime_context.builder is not None:
                job.builder = runtime_context.builder
            if job.outdir is not None:
                self.output_dirs.add(job.outdir)
            self.allocate(rsc, logger)
            future = pool_executor.submit(job.run, runtime_context)
            callback = functools.partial(self.job_done_callback, rsc, logger)
            # Callback will be invoked in a thread on the submitting process (but not the thread that submitted, this
            # clarification is mostly for process pool executors)
            future.add_done_callback(callback)
            submitted_futures.add(future)
        return submitted_futures

    def wait_for_completion(self, futures, logger):
        """
        Using concurrent.futures.wait, wait for one of the futures in the set to complete, remove finished futures from
        the set, and check if any exceptions occurred.
        :param futures: set: A set of futures on which to wait
        :param logger: logger where messages shall be logged
        :return: The set of futures that is not yet done
        """
        logger.debug('wait_for_completion with {} futures'.format(len(futures)))
        wait_results = wait(futures, return_when=FIRST_COMPLETED)
        # wait returns a NamedTuple of done and not_done.
        return wait_results.not_done

    def enqueue_jobs_from_iterator(self, job_iterator, logger, runtime_context, pool_executor):
        """
        Phase 1: Iterate over jobs, and queue them for execution. When the iterator returns None, that indicates
        progress is blocked until earlier jobs complete. At that point, this method starts jobs from the
        queue, submitting each as a Future to the pool_executor. Returns the set of submitted futures for phase 2
        to watch.

        :param job_iterator: iterator that yields cwltool Jobs
        :param logger: logger where messages shall be logged
        :param runtime_context: cwltool RuntimeContext: to provide to the job
        :param pool_executor: A concurrent.futures.Executor on which to run job functions
        :return: set of Futures for jobs that have been submitted off the queue
        """
        futures = set()
        iterator_exhausted = False
        while not iterator_exhausted:
            # Take the lock because we are modifying the job and processing the queue, which will consume resources
            with runtime_context.workflow_eval_lock:
                try:
                    job = next(job_iterator)
                    if job:
                        self.raise_if_oversized(job)
                        self.jrq.enqueue(job)
                    else:
                        # job is None. More to come, but depend on queued jobs completing, so start what we can
                        submitted = self.start_queued_jobs(pool_executor, logger, runtime_context)
                        futures.update(submitted)
                except StopIteration:
                    # No more jobs to queue.
                    iterator_exhausted = True
            # If jobs have been submitted to the queue, wait for one to finish
            # wait_for_completion must not have the lock, since jobs finishing will acquire it to provide their result
            futures = self.wait_for_completion(futures, logger)
            self.raise_if_exception_queued(futures, logger)
        return futures

    def drain_queue(self, logger, runtime_context, pool_executor, futures):
        """
        Start queued jobs and wait for all futures to complete
        :param logger: logger where messages shall be logged
        :param runtime_context: cwltool RuntimeContext: to provide to the job
        :param pool_executor: concurrent.futures.Executor on which to run job functions
        :param futures: set of Futures for jobs already started
        :return: None
        """
        finished = False
        while not finished:
            with runtime_context.workflow_eval_lock:
                # Taking the lock to work the queue
                submitted = self.start_queued_jobs(pool_executor, logger, runtime_context)  # submits jobs as futures
                futures.update(submitted)
            # wait_for_completion must not have the lock, since jobs finishing will acquire it to provide their result
            futures = self.wait_for_completion(futures, logger)
            self.raise_if_exception_queued(futures, logger)
            with runtime_context.workflow_eval_lock:
                # Check if we're done with pending jobs and submitted jobs
                if not futures and self.jrq.is_empty():
                    finished = True

    def run_jobs(self, process, job_order_object, logger, runtime_context):
        """
        Concrete implementation of JobExecutor.run_jobs, the primary entry point for this class.
        :param process: cwltool.process.Process: The process object on which to call .job() yielding jobs
        :param job_order_object: dict: The CWL job order
        :param logger: logger where messages shall be logged
        :param runtime_context: cwltool RuntimeContext: to provide to the job
        :return: None
        """
        logger.debug('Starting ThreadPoolJobExecutor.run_jobs: total_resources={}, max_workers={}'.format(
            self.total_resources, self.max_workers))
        if runtime_context.workflow_eval_lock is None:
            raise WorkflowException("runtimeContext.workflow_eval_lock must not be None")
        # Wrap in an Executor context. This ensures that the executor waits for tasks to finish before shutting down
        job_iterator = process.job(job_order_object, self.output_callback, runtime_context)
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool_executor:
            futures = self.enqueue_jobs_from_iterator(job_iterator, logger, runtime_context, pool_executor)
            self.drain_queue(logger, runtime_context, pool_executor, futures)
        logger.debug('Finishing ThreadPoolExecutor.run_jobs: total_resources={}, available_resources={}'.format(
            self.total_resources, self.available_resources))
