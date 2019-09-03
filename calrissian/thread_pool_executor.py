from cwltool.executors import *
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from queue import Queue
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

    def pop_runnable_jobs(self, resource_limit, priority=Resources.RAM, descending=False):
        """
        Collect a set of jobs within the specified limit and return them.
        May return an empty set if nothing fits in the resource limit
        :param resource_limit: A Resource object
        :return:
        """
        runnable_jobs = set()
        for job, resource in sorted(self.jobs.items(), key=lambda item: getattr(item[1], priority), reverse=descending):
            if resource_limit - resource >= Resources.EMPTY:
                runnable_jobs.add(job)
                resource_limit = resource_limit - resource
        for job in runnable_jobs:
            self.jobs.pop(job)
        return runnable_jobs


class ThreadPoolJobExecutor(JobExecutor):

    def __init__(self, total_ram, total_cpu, max_workers=None):
        super(ThreadPoolJobExecutor, self).__init__()
        self.pool_executor = ThreadPoolExecutor(max_workers=max_workers)
        # self.futures = set()
        self.job_queue = JobResourceQueue()
        self.total_resources = Resources(total_ram, total_cpu)
        self.allocated_resources = Resources()

    def run_job(self, job, runtime_context):
        # This could return a context or a lock
        print('run job', getattr(job,'id',None))
        if job:
            # TODO: Check for available resources
            job.run(runtime_context)
        return runtime_context

    def job_callback(self, future):
        # called on the thread that added the callback
        # Should be able to acquire the lock here.
        runtime_context = future.result()
        with runtime_context.workflow_eval_lock:
            print('Update allocations')

    def process_queue(self):
        # pull the first thing off the queue
        # See if it fits. if not keep looking
        # submit it as a future
        # return
        runnable_jobs = self.job_queue.pop_runnable_jobs()

        future = self.pool_executor.submit(self.run_job, job, runtime_context)
        future.add_done_callback(self.job_callback)
        futures.add(future)  # happens with lock

    def run_jobs(self, process, job_order_object, logger, runtime_context):
        lock = runtime_context.workflow_eval_lock
        jobs = process.job(job_order_object, self.output_callback, runtime_context)
        if lock is None:
            raise WorkflowException("runtimeContext.workflow_eval_lock must not be None")
        futures = set()
        with lock:
            print('have lock in run_jobs')
            # put the job in the queue
            for job in jobs:
                self.job_queue.add(job) # May be none if nothing to do
                self.process_queue()
                # Produce a future for each job
        print('releasing lock in run_jobs')
        # wait for a job to complete
        while True:
            wait_results = wait(futures, return_when=FIRST_COMPLETED)
            # waiting here for something to finish
            # a job finished
            with lock:
                print('run_jobs has lock')
                if wait_results.done:
                    futures = futures - wait_results.done
                if not futures:
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
                     Job(1),
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
