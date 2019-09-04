from cwltool.executors import *
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import functools
import time


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
        self.jobs = dict()

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

    def pop_runnable_jobs(self, resource_limit, priority=Resources.RAM, descending=False):
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
        self.futures = set()
        self.total_resources = Resources(total_ram, total_cpu)
        self.allocated_resources = Resources()

    @property
    def available_resources(self):
        return self.total_resources - self.allocated_resources

    def run_job(self, job, runtime_context, rsc):
        print('run job', getattr(job,'id',None))
        if job:
            job.run(runtime_context)

    def allocate(self, rsc, lock):
        # Must be called with the lock but its recursive so that should be fine
        with lock:
            print('allocate lock', rsc)
            self.total_resources = self.total_resources - rsc

    def restore(self, rsc, lock, future):
        # Must be called with the lock but its recursive so that should be fine
        with lock:
            print('restore lock', rsc)
            self.total_resources = self.total_resources + rsc

    def process_queue(self, runtime_context, futures):
        """
        Asks the queue for jobs that can run in the currently available resources,
        runs those jobs using the pool executor, and returns
        :return: None
        """
        runnable_jobs = self.jrq.pop_runnable_jobs(self.available_resources) # Removes jobs from the queue
        for job, rsc in runnable_jobs.items():
            self.allocate(rsc, runtime_context.workflow_eval_lock)
            future = self.pool_executor.submit(self.run_job, job, runtime_context, rsc)
            restore_callback = functools.partial(self.restore, rsc, runtime_context.workflow_eval_lock)
            future.add_done_callback(restore_callback)
            futures.add(future)

    def run_jobs(self, process, job_order_object, logger, runtime_context):
        jobs = process.job(job_order_object, self.output_callback, runtime_context)
        if runtime_context.workflow_eval_lock is None:
            raise WorkflowException("runtimeContext.workflow_eval_lock must not be None")
        with runtime_context.workflow_eval_lock:
            print('have lock in run_jobs')
            # put the job in the queue
            for job in jobs:
                self.jrq.add(job) # May be none if nothing to do
            # After jobs iterator is exhausted, all jobs will have been queued
        print('releasing lock in run_jobs')
        print('queue size is', len(self.jrq.jobs))
        # wait for a job to complete
        futures = set()
        while True:
            with runtime_context.workflow_eval_lock:
                self.process_queue(runtime_context, futures)  # Populates futures

            # Wait for one to complete
            wait_results = wait(futures, return_when=FIRST_COMPLETED)

            # a job finished
            with runtime_context.workflow_eval_lock:
                if wait_results.done:
                    futures = futures - wait_results.done
                # Check if we're done with pending jobs and submitted jobs
                if not futures and self.jrq.is_empty():
                    break
        print('exiting')


class Builder(object):

    def __init__(self):
        self.resources = {'cpu': 1, 'ram': 1000}


class Job(object):

    def __init__(self, id):
        self.id = id
        self.builder = Builder()

    def run(self, runtime_context):
        print('started', self.id)
        time.sleep(1)
        # Finish by acquiring lock
        with runtime_context.workflow_eval_lock:
            print('finished with lock', self.id)


class Process(object):

    def __init__(self):
        self.jobs = [Job(1),
                     Job(3),
                     Job(2),
                     None,
                     Job(4)]

    def job(self, job_order_object, output_callback, runtime_context):
        for j in self.jobs:
            yield j


class RuntimeContext(object):

    def __init__(self):
        self.workflow_eval_lock = threading.Condition(threading.RLock())


def main():
    executor = ThreadPoolJobExecutor(total_cpu=2, total_ram=4000, max_workers=3)
    runtime_context = RuntimeContext()
    process = Process()
    job_order_object = {}
    executor.run_jobs(process, job_order_object, None, runtime_context)

if __name__ == '__main__':
    main()
