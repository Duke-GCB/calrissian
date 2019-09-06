from thread_pool_executor import ThreadPoolJobExecutor
import logging
import time
import threading

log = logging.getLogger("calrissian.executor")


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
        time.sleep(5)
        if self.id == 86:
            raise Exception('Fail')
        # Finish by acquiring lock
        with runtime_context.workflow_eval_lock:
            log.debug('finishing {}'.format(self.id))
            time.sleep(3)


class Process(object):

    def __init__(self):
        self.jobs = [Job(1, 8, 100),
                     Job(6, 1, 100),
                     Job(3, 2, 200),
                     Job(2, 4, 200),
                     None,
                     None,
                     None,
                     Job(4, 16, 800),
                     ]

    def job(self, job_order_object, output_callback, runtime_context):
        for j in self.jobs:
            yield j


class RuntimeContext(object):

    def __init__(self):
        self.workflow_eval_lock = threading.Condition(threading.RLock())
        self.builder = None


def main():
    logging.getLogger('calrissian.executor'.format(log)).setLevel(logging.DEBUG)
    logging.getLogger('calrissian.executor'.format(log)).addHandler(logging.StreamHandler())
    executor = ThreadPoolJobExecutor(total_cpu=16, total_ram=800, max_workers=5)
    runtime_context = RuntimeContext()
    process = Process()
    job_order_object = {}
    executor.run_jobs(process, job_order_object, None, runtime_context)

if __name__ == '__main__':
    main()
