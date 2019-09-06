import functools
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from queue import Queue

from cwltool.errors import WorkflowException
from cwltool.executors import JobExecutor
from schema_salad.validate import ValidationException

log = logging.getLogger("calrissian.executor")


class JobAlreadyExistsException(Exception):
    pass


class Resources(object):
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
    Contains a dictionary of jobs, mapping to their resources
    """

    def __init__(self, priority=Resources.RAM, descending=False):
        self.jobs = dict()
        self.priority = priority
        self.descending = descending

    def add(self, job):
        """
        Add the job and extract its resources
        :param job:
        :return:
        """
        if job in self.jobs:
            raise JobAlreadyExistsException('Job already exists')
        if job:
            self.jobs[job] = Resources.from_job(job)

    def is_empty(self):
        return len(self.jobs) == 0

    def sorted_jobs(self):
        return sorted(self.jobs.items(), key=lambda item: getattr(item[1], self.priority), reverse=self.descending)

    def pop_runnable_jobs(self, resource_limit):
        """
        Collect a dictionary of jobs and resources within the specified limit and return them.
        May return an empty dictionary if nothing fits in the resource limit
        :param resource_limit: A Resource object
        :return: Dictionary where jobs are keys and their resources are values
        """
        runnable_jobs = {}
        for job, resource in self.sorted_jobs():
            if resource_limit - resource >= Resources.EMPTY:
                runnable_jobs[job] = resource
                resource_limit = resource_limit - resource
        for job in runnable_jobs:
            self.jobs.pop(job)
        return runnable_jobs

    def __str__(self):
        return ' '.join([str(j) for j in self.jobs.keys()])


class ThreadPoolJobExecutor(JobExecutor):

    def __init__(self, total_ram, total_cpu, max_workers=None):
        super(ThreadPoolJobExecutor, self).__init__()
        self.pool_executor = ThreadPoolExecutor(max_workers=max_workers)
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

    def run_job(self, job, runtime_context):
        if job:
            job.run(runtime_context)

    def job_callback(self, rsc, future):
        # Note that job callbacks are invoked on a thread belonging to the process that added them
        # So it's not the main thread. If the callback itself raises an Exception, that Exception is logged and ignored
        # If we want to stop executing because of an exception we do have some other cleanup to do
        # The done_callback is invoked on a background thread.
        self.restore(rsc)  # Always restore the resources

        # Exit now if the future was cancelled. If we call result() or exception() on a cancelled future, that call
        # will raise a CancelledError
        if future.cancelled():
            return

        ex = future.exception()  # raises a CancelledError if future was cancelled
        if ex:
            # The Queue is thread safe so we dont need a lock, even though we're running on a background thread
            self.exceptions.put(ex)

    def handle_queued_exceptions(self, pending_futures):
        """
        Check if we have any queued exceptions after futures finished. If so, cancel pending futures and raise the
        exception
        :param pending_futures: A set() of futures that should be cancelled if an exception occurred
        :return: None
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
            ex = self.exceptions.get()
            try:
                raise ex
            except (WorkflowException, ValidationException):
                raise
            except Exception as err:
                log.exception("Got workflow error")
                raise WorkflowException(str(err)) from err

    def raise_if_too_big(self, job):
        """
        Raise an exception if a job is too big to fit in the entire available resources

        :param job:
        :return:
        """
        rsc = Resources.from_job(job)
        if not rsc <= self.total_resources:
            raise WorkflowException('Job {} requires resources {} that exceed total resources {}'.
                                    format(job, rsc, self.total_resources))

    def allocate(self, rsc):
        with self.resources_lock:
            self.allocated_resources += rsc
            log.debug('allocated {}, available {}'.format(rsc, self.available_resources))

    def restore(self, rsc):
        with self.resources_lock:
            self.allocated_resources -= rsc
            log.debug('restored {}, available {}'.format(rsc, self.available_resources))

    def process_queue(self, runtime_context, futures, raise_on_unavalable=False):
        """
        Asks the queue for jobs that can run in the currently available resources,
        runs those jobs using the pool executor, and returns
        :return: None
        """
        runnable_jobs = self.jrq.pop_runnable_jobs(self.available_resources)  # Removes jobs from the queue
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
            future = self.pool_executor.submit(self.run_job, job, runtime_context)
            callback = functools.partial(self.job_callback, rsc)

            # Done callbacks are called in a thread on the process that executed them (but not the thread that called them)
            future.add_done_callback(callback)
            futures.add(future)

    def wait_for_future_completion(self, futures):
        log.debug('wait_for_future_completion with {} futures'.format(len(futures)))
        wait_results = wait(futures, return_when=FIRST_COMPLETED)
        [futures.remove(finished) for finished in wait_results.done]
        self.handle_queued_exceptions(wait_results.not_done)

    def run_jobs(self, process, job_order_object, logger, runtime_context):
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
                self.raise_if_too_big(job)
                self.jrq.add(job)
            else:
                # The job iterator yielded None. That means we're not done with the workflow but a dependency has
                # not yet finished
                self.process_queue(runtime_context, futures, raise_on_unavalable=True)
                runtime_context.workflow_eval_lock.release()
                # This currently deadlocks because we have the lock but the finishing job can't finish
                self.wait_for_future_completion(futures)
                runtime_context.workflow_eval_lock.acquire()
                log.debug('Job iterator yielded None this iteration')
        runtime_context.workflow_eval_lock.release()

        # At this point, we have exhausted the job iterator, and every job has been queued (or run)
        # so work the job queue until it is empty and all futures have completed
        while True:
            with runtime_context.workflow_eval_lock:
                # Taking the lock to process the queue
                self.process_queue(runtime_context, futures)  # submits jobs as futures
            # Wait for one to complete
            self.wait_for_future_completion(futures)
            with runtime_context.workflow_eval_lock:  # Why do we need the lock here?
                # Check if we're done with pending jobs and submitted jobs
                if not futures and self.jrq.is_empty():
                    break

