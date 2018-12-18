#!/usr/bin/env python
from __future__ import print_function
import threading

from cwltool.executors import JobExecutor
from cwltool.main import main

from cwltool.errors import WorkflowException
from cwltool.job import JobBase  # pylint: disable=unused-import
from cwltool.loghandler import _logger


class KubernetesJob(object):

    def __init__(self, job, runtime_context, executor):
        self.job = job
        self.runtime_context = runtime_context
        self.executor = executor

    def start(self):
        print('start {} {}'.format(self.job, self.runtime_context))
        print('TODO: RUN THE JOB in kubernetes')
        self.job.run(self.runtime_context)
        self.finish()

    def finish(self):
        print('finishing the job')
        with self.runtime_context.workflow_eval_lock:
            # Remove self
            self.executor.remove_k8s_job(self)
            self.runtime_context.workflow_eval_lock.notifyAll()

class SeawallExecutor(JobExecutor):
    """
    Inspired by MultithreadedJobExecutor
    https://github.com/common-workflow-language/cwltool/blob/7c7615c44b80f8e76e659433f8c7875603ae0b25/cwltool/executors.py#L193
    """

    def __init__(self):
        super(SeawallExecutor, self).__init__()
        self.k8s_jobs = set()
        self.pending_jobs = []
        self.pending_jobs_lock = threading.Lock()

    def wait_for_next_completion(self, runtime_context):
        pass

    def remove_k8s_job(self, k8s_job):
        self.k8s_jobs.remove(k8s_job)

    def run_job(self, job, runtime_context):
        if job is not None:
            with self.pending_jobs_lock:
                self.pending_jobs.append(job)

        while self.pending_jobs:
            with self.pending_jobs_lock:
                job = self.pending_jobs[0]
                self.pending_jobs.remove(job)
            k8s_job = KubernetesJob(job, runtime_context, self)
            self.k8s_jobs.add(k8s_job)
            k8s_job.start()

    def run_jobs(self,
                 process,           # type: Process
                 job_order_object,  # type: Dict[Text, Any]
                 logger,
                 runtime_context     # type: RuntimeContext
                ):

        jobiter = process.job(job_order_object, self.output_callback,
                              runtime_context)

        if runtime_context.workflow_eval_lock is None:
            raise WorkflowException(
                "runtimeContext.workflow_eval_lock must not be None")

        runtime_context.workflow_eval_lock.acquire()
        for job in jobiter:
            if job is not None:
                if isinstance(job, JobBase):
                    job.builder = runtime_context.builder or job.builder
                    if job.outdir is not None:
                        self.output_dirs.add(job.outdir)

            self.run_job(job, runtime_context)

            if job is None:
                if self.k8s_jobs:
                    self.wait_for_next_completion(runtime_context)
                else:
                    logger.error("Workflow cannot make any more progress.")
                    break

        while self.k8s_jobs:
            self.wait_for_next_completion(runtime_context)
        runtime_context.workflow_eval_lock.release()

argslist = [
    '--outdir=cwl/out',
    '--tmp-outdir-prefix=cwl/tmp/tmpout',
    '--tmpdir-prefix=cwl/tmp/tmp',
    'cwl/revsort-single.cwl',
    'cwl/revsort-single-job.json'
]

main(argslist, executor=SeawallExecutor())
