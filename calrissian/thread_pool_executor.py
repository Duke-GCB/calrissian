import functools
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from queue import Queue

from cwltool.errors import WorkflowException
from cwltool.executors import JobExecutor
from schema_salad.validate import ValidationException

log = logging.getLogger("calrissian.executor")


class DuplicateJobException(Exception):
    pass


class OversizedJobException(Exception):
    pass


class InconsistentResourcesException(Exception):
    pass


class Resources(object):
    """
    Class to encapsulate compute resources and provide arithmetic operations and comparisons
    """
    RAM = 'ram'
    CPU = 'cpu'

    def __init__(self, ram=0, cpu=0):
        self.ram = ram
        self.cpu = cpu

    def __sub__(self, other):
        ram = self.ram - other.ram
        cpu = self.cpu - other.cpu
        return Resources(ram, cpu)

    def __add__(self, other):
        ram = self.ram + other.ram
        cpu = self.cpu + other.cpu
        return Resources(ram, cpu)

    def __neg__(self):
        return Resources(-self.ram, -self.cpu)

    def __lt__(self, other):
        return self.ram < other.ram and self.cpu < other.cpu

    def __gt__(self, other):
        return self.ram > other.ram and self.cpu > other.cpu

    def __eq__(self, other):
        return self.ram == other.ram and self.cpu == other.cpu

    def __ge__(self, other):
        return self.ram >= other.ram and self.cpu >= other.cpu

    def __le__(self, other):
        return self.ram <= other.ram and self.cpu <= other.cpu

    def __str__(self):
        return '[ram: {}, cpu: {}]'.format(self.ram, self.cpu)

    @classmethod
    def from_job(cls, job):
        ram = job.builder.resources.get(cls.RAM, 0)
        cpu = job.builder.resources.get(cls.CPU, 0)
        return cls(ram, cpu)


Resources.EMPTY = Resources(0, 0)


class JobResourceQueue(object):
    """
    Contains a dictionary of jobs, mapped to their resources.
    Provides an interface for getting a subset of jobs that fit within a resource limit
    """

    def __init__(self, priority=Resources.RAM, descending=False):
        """
        Create a JobResourceQueue
        :param priority: Resources.RAM or Resources.CPU - Used as a sort key when de-queuing jobs
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
        Produces a list of the jobs in the queue, ordered by self.priority (RAM or CPU) and ascending or descending
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

    def __str__(self):
        return ' '.join([str(j) for j in self.jobs.keys()])


class ThreadPoolJobExecutor(JobExecutor):
    """
    A cwltool JobExecutor subclass that uses concurrent.futures.ThreadPoolExecutor

    concurrent.futures was introduced in Python 3.2 and provides high-level interfaces (Executor, Future) for launching
    and managing asynchronous and parallel tasks. The ThreadPoolExecutor maintains a pool of reusable threads to execute
    tasks, reducing the overall number of threads that are created in the cwltool process.

    Relevant: https://github.com/common-workflow-language/cwltool/issues/888
    """

    def __init__(self, total_ram, total_cpu, max_workers=None):
        """
        Initialize a ThreadPoolJobExecutor
        :param total_ram: RAM limit in megabytes for concurrent jobs
        :param total_cpu: CPU core count limit for concurrent jobs
        :param max_workers: Number of worker threads to create. Set to None to use Python's default of 5xCPU count, which
        should be sufficient. Setting max_workers too low can cause deadlocks.

        See https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
        """
        super(ThreadPoolJobExecutor, self).__init__()
        self.max_workers = max_workers
        self.jrq = JobResourceQueue()
        self.exceptions = Queue()
        self.futures = set()
        self.total_resources = Resources(total_ram, total_cpu)
        self.allocated_resources = Resources()
        self.resources_lock = threading.RLock()
        log.debug('initialize ThreadPoolJobExecutor: total_resources={}, max_workers={}'.format(self.total_resources, max_workers))

    @property
    def available_resources(self):
        with self.resources_lock:
            return self.total_resources - self.allocated_resources

    def job_done_callback(self, rsc, future):
        """
        Callback to run after a job is finished to restore reserved resources and check for exceptions.
        Expected to be called as part of the Future.add_done_callback(). The callback is invoked on a background
        thread. If the callback itself raises an Exception, that Exception is logged and ignored, so we instead
        queue exceptions and re-raise from the main thread.

        :param rsc: Resources used by the job to return to our available resources.
        :param future: A concurrent.futures.Future representing the finished task. May be in cancelled or done states
        """

        # Always restore the resources
        self.restore(rsc)

        # if the future was cancelled, there is no more work to do. Bail out now because calling result() or
        # exception() on a cancelled future would raise a CancelledError in this scope.
        if future.cancelled():
            return

        # Check if the future raised an exception - may return None
        if future.exception():
            # The Queue is thread safe so we dont need a lock, even though we're running on a background thread
            self.exceptions.put(future.exception())

    def raise_queued_exceptions(self, pending_futures):
        """
        Method to run on the main thread that will raise a queued exception added by job_done_callback and
        cancel any outstanding futures
        :param pending_futures: set of any futures that should be cancelled if we're about to raise an exception
        """
        # Code translated from SingleJobExecutor.run_jobs
        # It raises WorkflowExceptions and ValidationException directly.
        # Other Exceptions are converted to WorkflowException
        if not self.exceptions.empty():
            # There's at least one exception, cancel all pending jobs
            # Note that cancel will only matter if there aren't enough available threads to start processing the job in
            # the first place. Once the function starts running it cannot be cancelled.
            for f in pending_futures:
                f.cancel()
            # TODO: Do we need to wait for cancellations to process?
            ex = self.exceptions.get()
            try:
                raise ex
            except (WorkflowException, ValidationException):
                raise
            except Exception as err:
                log.exception("Got workflow error")
                raise WorkflowException(str(err)) from err

    def raise_if_oversized(self, job):
        """
        Raise an exception if a job does not fit within total_resources
        :param job: Job to check resources
        """
        rsc = Resources.from_job(job)
        if not rsc <= self.total_resources:
            raise OversizedJobException('Job {} requires resources {} that exceed total resources {}'.
                                        format(job, rsc, self.total_resources))

    def allocate(self, rsc):
        """
        Reserve resources from the total. Raises InconsistentResourcesException if available becomes negative
        :param rsc: A Resources object to reserve from the total.
        """
        with self.resources_lock:
            self.allocated_resources += rsc
            log.debug('allocated {}, available {}'.format(rsc, self.available_resources))
            if self.available_resources <= Resources.EMPTY:
                raise InconsistentResourcesException(str(self.available_resources))

    def restore(self, rsc):
        """
        Restore resources to the total. Raises InconsistentResourcesException if available becomes negative
        :param rsc: A Resources object to restore to the total
        """
        with self.resources_lock:
            self.allocated_resources -= rsc
            log.debug('restored {}, available {}'.format(rsc, self.available_resources))
            if self.available_resources <= Resources.EMPTY:
                raise InconsistentResourcesException(str(self.available_resources))

    def process_queue(self, pool_executor, runtime_context, futures, raise_on_unavalable=False):
        """
        Pulls jobs off the queue in groups that fit in currently available resources, allocates resources, and submits
        jobs to the pool_executor as Futures. Attaches a callback to each future to clean up (e.g. check
        for execptions, restore allocated resources)
        :param pool_executor: concurrent.futures.Executor: where job callables shall be submitted
        :param runtime_context: cwltool RuntimeContext: to provide to the job
        :param futures: set: set to add submitted futures to (refactor this)
        :param raise_on_unavalable: Boolean: If True, raise an exception if no jobs can be run.
        :return: None
        """
        runnable_jobs = self.jrq.dequeue(self.available_resources)  # Removes jobs from the queue
        if raise_on_unavalable and not runnable_jobs and not self.jrq.is_empty():
            # Queue is not empty and we have no runnable jobs
            raise WorkflowException(
                'Jobs queued but resources available {} cannot run any queued job: {}'.format(self.available_resources,
                                                                                              self.jrq))
        for job, rsc in runnable_jobs.items():
            if runtime_context.builder is not None:
                job.builder = runtime_context.builder
            if job.outdir is not None:
                self.output_dirs.add(job.outdir)
            self.allocate(rsc)
            future = pool_executor.submit(job.run, runtime_context)
            callback = functools.partial(self.job_done_callback, rsc)
            # Callback will be invoked in a thread on the submitting process (but not the thread that submitted, this
            # clarification is mostly for process pool executors)
            future.add_done_callback(callback)
            futures.add(future)

    def wait_for_future_completion(self, futures):
        """
        Using concurrent.futures.wait, wait for one of the futures in the set to complete, remove finished futures from
        the set, and check if any exceptions occurred.
        :param futures: set: A set of futures on which to wait
        :return: None
        """
        log.debug('wait_for_future_completion with {} futures'.format(len(futures)))
        wait_results = wait(futures, return_when=FIRST_COMPLETED)
        # wait returns a NamedTuple (done, not_done).
        [futures.remove(done) for done in wait_results.done]
        # Check for queued exceptions and provide the not_done futures to be cancelled if exceptions occurred
        self.raise_queued_exceptions(wait_results.not_done)

    def run_jobs(self, process, job_order_object, logger, runtime_context):
        """
        Concrete implementation of JobExecutor.run_jobs, the primary entry point for this class. Runs in two phases:

        1. Adds jobs to the queue by looping over process.job() to generate them. If the generator returns None
        (instead of a Job), some previous job must complete before future jobs can be generated. When this happens,
        start executing jobs (process_queue). Phase 1 is complete when process.job() is exhausted.

        2. Loop until the queue is empty and all submitted jobs have completed.
        :param process: cwltool.process.Process: The process object on which to call .job() yielding jobs
        :param job_order_object: dict: The CWL job order
        :param logger: logger where messages shall be logged
        :param runtime_context: cwltool RuntimeContext: to provide to the job
        :return: None
        """
        # Wrap in an Executor context. This ensures that the executor waits for tasks to finish,
        # then shuts down cleaning up resources
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool_executor:
            if runtime_context.workflow_eval_lock is None:
                raise WorkflowException("runtimeContext.workflow_eval_lock must not be None")
            job_iterator = process.job(job_order_object, self.output_callback, runtime_context)
            futures = set()

            # Phase 1: loop over the job iterator, adding jobs to the queue
            # We take the lock because we are modifying the job and processing the queue, which will consume resources
            runtime_context.workflow_eval_lock.acquire()
            # put the job in the queue
            for job in job_iterator:
                if job is not None:
                    # Set up builder here before adding to queue, which will get job's resource needs
                    if runtime_context.builder is not None:
                        job.builder = runtime_context.builder
                    if job.outdir is not None:
                        self.output_dirs.add(job.outdir)
                    self.raise_if_oversized(job)
                    self.jrq.enqueue(job)
                else:
                    # The job iterator yielded None. That means we're not done with the workflow but a dependency has
                    # not yet finished
                    self.process_queue(pool_executor, runtime_context, futures, raise_on_unavalable=True)
                    runtime_context.workflow_eval_lock.release()
                    # This currently deadlocks because we have the lock but the finishing job can't finish
                    self.wait_for_future_completion(futures)
                    runtime_context.workflow_eval_lock.acquire()
            runtime_context.workflow_eval_lock.release()

            # Phase 2: process the queue and wait for all jobs to copmlete
            while True:
                with runtime_context.workflow_eval_lock:
                    # Taking the lock to process the queue
                    self.process_queue(pool_executor, runtime_context, futures)  # submits jobs as futures
                # Wait for one to complete
                self.wait_for_future_completion(futures)
                with runtime_context.workflow_eval_lock:  # Why do we need the lock here?
                    # Check if we're done with pending jobs and submitted jobs
                    if not futures and self.jrq.is_empty():
                        break

