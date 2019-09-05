from cwltool.executors import JobExecutor
from cwltool.errors import WorkflowException
from schema_salad.validate import ValidationException
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from queue import Queue
import functools
import time
import logging

log = logging.getLogger("calrissian.executor")


class JobAlreadyExistsException(Exception):
    pass

import threading


def tname(msg):
    log.debug('{} {}'.format(msg, threading.current_thread()))


def raise_if_not_main():
    if threading.current_thread() != threading.main_thread():
        raise Exception('this should only be called from main thread')


def raise_if_main():
    if threading.current_thread() == threading.main_thread():
        raise Exception('this should not be called from main thread')


def report_state(future):
    if future.done():
        log.info('done')
    if future.cancelled():
        log.info('cancelled')
    if future.running():
        log.info('running')

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
        return self > other or self == other

    def __le__(self, other):
        return self < other or self == other

    def __str__(self):
        return 'ram: {}, cpu: {}'.format(self.ram, self.cpu)

    @classmethod
    def from_job(cls, job):
        ram = job.builder.resources.get(cls.RAM, 0)
        cpu = job.builder.resources.get(cls.CPU, 0)
        return cls(ram, cpu)


Resources.EMPTY = Resources(0,0)


class JobResourceQueue(object):
    """
    Contains a dictionary of jobs, mapping to their resources
    """

    def __init__(self):
        raise_if_not_main()
        self.jobs = dict()

    def add(self, job):
        raise_if_not_main()
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
        raise_if_not_main()
        return len(self.jobs) == 0

    def pop_runnable_jobs(self, resource_limit, priority=Resources.RAM, descending=False):
        raise_if_not_main()
        """
        Collect a dictionary of jobs and resources within the specified limit and return them.
        May return an empty dictionary if nothing fits in the resource limit
        :param resource_limit: A Resource object
        :return: Dictionary where jobs are keys and their resources are values
        """
        runnable_jobs = {}
        for job, resource in sorted(self.jobs.items(), key=lambda item: getattr(item[1], priority), reverse=descending):
            if resource_limit - resource >= Resources.EMPTY:
                runnable_jobs[job] = resource
                resource_limit = resource_limit - resource
        for job in runnable_jobs:
            self.jobs.pop(job)
        return runnable_jobs


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

    @property
    def available_resources(self):
        with self.resources_lock:
            return self.total_resources - self.allocated_resources

    def run_job(self, job, runtime_context):
        raise_if_main()
        log.debug('run job {}'.format(job))
        if job:
            job.run(runtime_context)

    def job_callback(self, rsc, future):
        # Note that job callbacks are invoked on a thread belonging to the process that added them
        # So it's not the main thread. If the callback itself raises an Exception, that Exception is logged and ignored
        # If we want to stop executing because of an exception we do have some other cleanup to do
        tname('job callback')
        report_state(future)
        # The done_callback is invoked on a background thread.
        self.restore(rsc) # Always restore the resources

        # Exit now if the future was cancelled. If we call result() or exception() on a cancelled future, that call
        # will raise a CancelledError
        if future.cancelled():
            return

        ex = future.exception() # raises a CancelledError if future was cancelled
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
        raise_if_not_main()
        # Code translated from SingleJobExecutor.run_jobs
        # It raises WorkflowExceptions and ValidationException directly.
        # Other Exceptions are converted to WorkflowException
        tname('raise_exceptions')
        if not self.exceptions.empty():
            tname('have exceptions')
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

    def allocate(self, rsc):
        with self.resources_lock:
            self.total_resources = self.total_resources - rsc
            log.info('allocated {}, available {}'.format(rsc, self.available_resources))

    def restore(self, rsc):
        with self.resources_lock:
            self.total_resources = self.total_resources + rsc
            log.info('restored {}, available {}'.format(rsc, self.available_resources))


    def process_queue(self, runtime_context, futures):
        """
        Asks the queue for jobs that can run in the currently available resources,
        runs those jobs using the pool executor, and returns
        :return: None
        """
        raise_if_not_main()
        log.info('processing queue')
        runnable_jobs = self.jrq.pop_runnable_jobs(self.available_resources) # Removes jobs from the queue
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

    def wait_for_completion(self, futures):
        log.info('wait with {} futures'.format(len(futures)))
        wait_results = wait(futures, return_when=FIRST_COMPLETED)
        [futures.remove(finished) for finished in wait_results.done]
        self.handle_queued_exceptions(wait_results.not_done)

    def run_jobs(self, process, job_order_object, logger, runtime_context):
        raise_if_not_main()
        if runtime_context.workflow_eval_lock is None:
            raise WorkflowException("runtimeContext.workflow_eval_lock must not be None")
        log.info('initial available {}'.format(self.available_resources))
        job_iterator = process.job(job_order_object, self.output_callback, runtime_context)
        futures = set()

        # Phase 1: loop over the job iterator, adding jobs to the queue
        # We take the lock because we are modifying the job and processing the queue, which will consume resources
        with runtime_context.workflow_eval_lock:
            # put the job in the queue
            for job in job_iterator:
                if job is not None:
                    # Set up builder here before adding to queue, which will get job's resource needs
                    if runtime_context.builder is not None:
                        job.builder = runtime_context.builder
                    if job.outdir is not None:
                        self.output_dirs.add(job.outdir)
                    self.jrq.add(job)
                else:
                    # The job iterator yielded None. That means we're not done with the workflow but a dependency has
                    # not yet finished
                    # TODO: Fail the workflow here if we couldn't start any futures. That suggests we don't have enough resources
                    self.process_queue(runtime_context, futures)
                    self.wait_for_completion(futures)
                    log.debug('Job iterator yielded no job this iteration')

        # At this point, we have exhausted the job iterator, and every job has been queued (or run)
        # so work the job queue until it is empty and all futures have completed
        while True:
            with runtime_context.workflow_eval_lock:
                # Taking the lock to process the queue
                self.process_queue(runtime_context, futures)  # submits jobs as futures
            # Wait for one to complete
            self.wait_for_completion(futures)
            with runtime_context.workflow_eval_lock:  # Why do we need the lock here?
                # Check if we're done with pending jobs and submitted jobs
                    if not futures and self.jrq.is_empty():
                        break
        log.info('exiting')
        log.info('final available {}'.format(self.available_resources))


class Builder(object):

    def __init__(self, cpu, ram):
        self.resources = {'cpu': cpu, 'ram': ram}

paused = False

class Job(object):

    def __init__(self, id, cpu, ram):
        self.id = id
        self.builder = Builder(cpu, ram)
        self.outdir = None

    def __str__(self):
        return 'id: {}'.format(self.id)

    def run(self, runtime_context):
        log.debug('started {}'.format(self.id))
        tname('run {}'.format(self.id))
        time.sleep(5)
        if self.id == 86:
            raise Exception('Fail')
        # Finish by acquiring lock
        with runtime_context.workflow_eval_lock:
            log.debug('finishing {}'.format(self.id))
            time.sleep(3)


class Process(object):

    # TODO: Make later jobs dependent on earlier jobs
    def __init__(self):
        self.jobs = [Job(1, 8, 100),
                     Job(86, 1, 100),
                     Job(3, 2, 200),
                     Job(2, 4, 200),
                     None,
                     None,
                     None,
                     Job(4, 16, 800),
                     ]

    def job(self, job_order_object, output_callback, runtime_context):
        raise_if_not_main()
        for j in self.jobs:
            yield j


class RuntimeContext(object):

    def __init__(self):
        self.workflow_eval_lock = threading.Condition(threading.RLock())
        self.builder = None


def main():
    logging.getLogger('calrissian.executor'.format(log)).setLevel(logging.INFO)
    logging.getLogger('calrissian.executor'.format(log)).addHandler(logging.StreamHandler())
    executor = ThreadPoolJobExecutor(total_cpu=16, total_ram=800, max_workers=5)
    runtime_context = RuntimeContext()
    process = Process()
    job_order_object = {}
    executor.run_jobs(process, job_order_object, None, runtime_context)

if __name__ == '__main__':
    main()
