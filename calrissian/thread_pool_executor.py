from cwltool.executors import *
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import time

class ThreadPoolJobExecutor(JobExecutor):
    def __init__(self, max_workers=None):
        super(ThreadPoolJobExecutor, self).__init__()
        self.pool_executor = ThreadPoolExecutor(max_workers=max_workers)
        # self.futures = set()

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

    def run_jobs(self, process, job_order_object, logger, runtime_context):
        lock = runtime_context.workflow_eval_lock
        jobs = process.job(job_order_object, self.output_callback, runtime_context)
        if lock is None:
            raise WorkflowException("runtimeContext.workflow_eval_lock must not be None")
        futures = set()
        with lock:
            print('have lock in run_jobs')
            for job in jobs:
                future = self.pool_executor.submit(self.run_job, job, runtime_context)
                future.add_done_callback(self.job_callback)
                futures.add(future) # happens with lock
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


class Job(object):

    def __init__(self, id):
        self.id = id

    def run(self, runtime_context):
        print('started', self.id)
        time.sleep(1)
        # Finish by acquiring lock
        with runtime_context.workflow_eval_lock:
            print('finished with lock', self.id)


class Process(object):

    def __init__(self):
        self.jobs = [Job('1'),
                     Job('2'),
                     Job('3'),
                     None,
                     Job('4')]

    def job(self, job_order_object, output_callback, runtime_context):
        for j in self.jobs:
            yield j


class RuntimeContext(object):

    def __init__(self):
        self.workflow_eval_lock = threading.Condition(threading.RLock())


def main():
    executor = ThreadPoolJobExecutor(max_workers=3)
    runtime_context = RuntimeContext()
    process = Process()
    job_order_object = {}

    executor.run_jobs(process, job_order_object, None, runtime_context)

if __name__ == '__main__':
    main()
