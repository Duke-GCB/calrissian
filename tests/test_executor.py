from concurrent.futures import Future, ThreadPoolExecutor
import threading
from unittest import TestCase
from unittest.mock import patch, call, Mock, create_autospec

from calrissian.executor import Resources, JobResourceQueue, ThreadPoolJobExecutor
from calrissian.executor import DuplicateJobException, OversizedJobException, InconsistentResourcesException
from cwltool.errors import WorkflowException


def make_mock_job(resources):
    return Mock(builder=Mock(resources={'ram': resources.ram, 'cores': resources.cores}))


class ResourcesTestCase(TestCase):

    def setUp(self):
        self.resource11 = Resources(1, 1, 1)
        self.resource22 = Resources(2, 2, 2)
        self.resource33 = Resources(3, 3, 3)
        self.resource21 = Resources(2, 1, 2)
        self.resource12 = Resources(1, 2, 1)
        self.resource_neg = Resources(-1, 0, 0)

    def test_init(self):
        self.assertEqual(self.resource11.cores, 1)
        self.assertEqual(self.resource11.ram, 1)
        self.assertEqual(self.resource22.cores, 2)
        self.assertEqual(self.resource22.ram, 2)
        self.assertEqual(self.resource33.cores, 3)
        self.assertEqual(self.resource33.ram, 3)
        self.assertEqual(self.resource21.cores, 1)
        self.assertEqual(self.resource21.ram, 2)

    def test_subtraction(self):
        result = self.resource33 - self.resource21
        self.assertEqual(result.ram, 1)
        self.assertEqual(result.cores, 2)

    def test_addition(self):
        result = self.resource11 + self.resource22
        self.assertEqual(result.ram, 3)
        self.assertEqual(result.cores, 3)

    def test_neg(self):
        result = - self.resource11
        self.assertEqual(result.ram, -1)
        self.assertEqual(result.cores, -1)

    def test_lt(self):
        self.assertTrue(self.resource11 < self.resource22)
        self.assertTrue(self.resource21 < self.resource33)
        self.assertFalse(self.resource11 < self.resource21)

    def test_gt(self):
        self.assertTrue(self.resource22 > self.resource11)
        self.assertTrue(self.resource33 > self.resource21)
        self.assertFalse(self.resource21 > self.resource11)

    def test_ge(self):
        self.assertTrue(self.resource21 >= self.resource11)
        self.assertFalse(self.resource22 >= self.resource33)

    def test_le(self):
        self.assertTrue(self.resource11 <= self.resource21)
        self.assertFalse(self.resource33 <= self.resource22)

    def test_eq(self):
        other = Resources(1, 1, 1)
        self.assertEqual(self.resource11, other)

    def test_from_job(self):
        mock_job = make_mock_job(Resources(4, 2, 1))
        result = Resources.from_job(mock_job)
        self.assertEqual(result.ram, 4)
        self.assertEqual(result.cores, 2)

    def test_from_job_defaults_empty_without_builder(self):
        mock_job = make_mock_job(Resources(4, 2, 1))
        del mock_job.builder
        result = Resources.from_job(mock_job)
        self.assertEqual(result.ram, 0)
        self.assertEqual(result.cores, 0)

    def test_from_dict(self):
        result = Resources.from_dict({'cores': 3, 'ram': 400, 'gpus': 1})
        self.assertEqual(result.cores, 3)
        self.assertEqual(result.ram, 400)
        self.assertEqual(result.gpus, 1)

    def test_min(self):
        result = Resources.min(self.resource21, self.resource12)
        self.assertEqual(result, self.resource11)

    def test_is_negative(self):
        self.assertFalse(self.resource11.is_negative())
        self.assertTrue(self.resource_neg.is_negative())

    def test_exceeds(self):
        self.assertTrue(self.resource21.exceeds(self.resource11))
        self.assertFalse(self.resource11.exceeds(self.resource21))
        self.assertFalse(self.resource21.exceeds(self.resource21))

    def test_empty(self):
        self.assertEqual(Resources.EMPTY.ram, 0)
        self.assertEqual(Resources.EMPTY.cores, 0)


class JobResourceQueueTestCase(TestCase):

    def setUp(self):
        self.jrq = JobResourceQueue()
        r100_4 = Resources(100, 4)
        r200_2 = Resources(200, 2)
        self.job100_4 = make_mock_job(r100_4)  # 100 RAM, 2 cores
        self.job200_2 = make_mock_job(r200_2)  # 200 RAM, 4 cores
        self.jobs = {self.job100_4: r100_4, self.job200_2: r200_2}

    def queue_jobs(self):
        for j in self.jobs:
            self.jrq.enqueue(j)

    def test_init(self):
        self.assertIsNotNone(self.jrq)

    def test_add(self):
        self.assertNotIn(self.job100_4, self.jrq.jobs)
        self.jrq.enqueue(self.job100_4)
        self.assertIn(self.job100_4, self.jrq.jobs)

    def test_add_raises_if_exists(self):
        self.jrq.enqueue(self.job100_4)
        with self.assertRaisesRegex(DuplicateJobException, 'Job already exists'):
            self.jrq.enqueue(self.job100_4)

    def test_dequeue_all_fit(self):
        limit = Resources(1000, 10)
        self.queue_jobs()
        runnable = self.jrq.dequeue(limit)
        self.assertEqual(runnable, self.jobs)

    def test_dequeue_none_fit_ram_too_small(self):
        limit = Resources(99, 10)
        self.queue_jobs()
        runnable = self.jrq.dequeue(limit)
        self.assertEqual(len(runnable), 0)

    def test_dequeue_none_fit_cores_too_small(self):
        limit = Resources(1000, 1)
        self.queue_jobs()
        runnable = self.jrq.dequeue(limit)
        self.assertEqual(len(runnable), 0)

    def test_dequeue_one_fits(self):
        limit = Resources(250, 3)
        self.queue_jobs()
        runnable = self.jrq.dequeue(limit)
        self.assertIn(self.job200_2, runnable)
        runnable = self.jrq.dequeue(limit)
        self.assertEqual(len(runnable), 0)

    def test_dequeue_one_at_a_time(self):
        limit = Resources(250, 5)
        self.queue_jobs()
        runnable = self.jrq.dequeue(limit)
        self.assertEqual(len(runnable), 1)
        runnable = self.jrq.dequeue(limit)
        self.assertEqual(len(runnable), 1)
        runnable = self.jrq.dequeue(limit)
        self.assertEqual(len(runnable), 0)

    def test_smallest_cores_first(self):
        self.jrq.priority = Resources.CORES
        self.jrq.descending = False
        limit = Resources(250, 5)
        self.queue_jobs()
        runnable = self.jrq.dequeue(limit)
        self.assertEqual(len(runnable), 1)
        self.assertIn(self.job200_2, runnable)

    def test_pop_runnable_job_largest_cores_first(self):
        self.jrq.priority = Resources.CORES
        self.jrq.descending = True
        limit = Resources(250, 5)
        self.queue_jobs()
        runnable = self.jrq.dequeue(limit)
        self.assertEqual(len(runnable), 1)
        self.assertIn(self.job100_4, runnable)

    def test_pop_runnable_job_smallest_ram_first(self):
        self.jrq.priority = Resources.RAM
        self.jrq.descending = False
        limit = Resources(250, 5)
        self.queue_jobs()
        runnable = self.jrq.dequeue(limit)
        self.assertEqual(len(runnable), 1)
        self.assertIn(self.job100_4, runnable)

    def test_pop_runnable_job_largest_ram_first(self):
        self.jrq.priority = Resources.RAM
        self.jrq.descending = True
        limit = Resources(250, 5)
        self.queue_jobs()
        runnable = self.jrq.dequeue(limit)
        self.assertEqual(len(runnable), 1)
        self.assertIn(self.job200_2, runnable)

    def test_pop_runnable_exact_fit(self):
        limit = Resources(300, 6)
        self.queue_jobs()
        runnable = self.jrq.dequeue(limit)
        self.assertEqual(runnable, self.jobs)

    def test_is_empty(self):
        self.assertTrue(self.jrq.is_empty())
        self.queue_jobs()
        self.assertFalse(self.jrq.is_empty())
        self.jrq.dequeue(Resources(300, 6))
        self.assertTrue(self.jrq.is_empty())


class ThreadPoolJobExecutorTestCase(TestCase):

    def setUp(self):
        self.executor = ThreadPoolJobExecutor(1000, 2, 2)
        self.workflow_exception = WorkflowException('workflow exception')
        self.logger = Mock()

    def test_init(self):
        expected_resources = Resources(1000, 2, 2)
        self.assertEqual(self.executor.total_resources, expected_resources)
        self.assertEqual(self.executor.available_resources, expected_resources)
        self.assertIsNone(self.executor.max_workers)
        self.assertIsNotNone(self.executor.jrq)
        self.assertIsNotNone(self.executor.exceptions)
        self.assertIsNotNone(self.executor.resources_lock)

    def test_select_resources_raises_if_exceeds(self):
        request = {
            'ramMin': 2000,
            'ramMax': 3000,
            'coresMin': 1,
            'coresMax': 1
        }
        with self.assertRaisesRegex(WorkflowException, 'exceed total'):
            self.executor.select_resources(request, Mock())

    def test_select_resources_returns_fit(self):
        request = {
            'ramMin': 500,
            'ramMax': 1500,
            'coresMin': 1,
            'coresMax': 1
        }

        result = self.executor.select_resources(request, Mock())
        self.assertEqual(result['ramMax'], 1000) # When ram max requested exceeds total, result should be total
        self.assertEqual(result['coresMax'], 1) # when cpu max requested is below total, result should be requested

    def test_requested_and_limit_resources(self):
        request = {
            'ramMin': 500,
            'ramMax': 1000,
            'coresMin': 1,
            'coresMax': 2
        }
        result = self.executor.select_resources(request, Mock())
        self.assertEqual(result['ram'], 500)
        self.assertEqual(result['cores'], 1)
        self.assertEqual(result['ramMax'], 1000)
        self.assertEqual(result['coresMax'], 2)

    def test_allocate(self):
        resource = Resources(200, 1, 1)
        self.executor.allocate(resource, self.logger)
        self.assertEqual(self.executor.available_resources, Resources(800, 1, 1))

    def test_restore(self):
        resource = Resources(200, 1, 1)
        self.executor.available_resources = Resources(800, 1, 1)
        self.executor.restore(resource, self.logger)
        self.assertEqual(self.executor.available_resources, self.executor.total_resources)

    def test_allocate_too_much_raises(self):
        resource = Resources(10000, 1)
        self.assertTrue(resource.exceeds(self.executor.available_resources))
        self.assertEqual(self.executor.available_resources, self.executor.total_resources)
        with self.assertRaisesRegex(InconsistentResourcesException, 'Available resources are negative'):
            self.executor.allocate(resource, self.logger)

    def test_restore_over_total_raises(self):
        self.assertEqual(self.executor.available_resources, self.executor.total_resources)
        with self.assertRaisesRegex(InconsistentResourcesException, 'Available resources exceeds total'):
            self.executor.restore(Resources(200, 1), self.logger)

    def test_restore_only_cores_over_total_raises(self):
        with self.assertRaisesRegex(InconsistentResourcesException, 'Available resources exceeds total'):
            self.executor.restore(Resources(0, 1), self.logger)

    @patch('calrissian.executor.wait')
    def test_raise_if_exception_queued_raises_and_waits(self, mock_wait):
        self.executor.exceptions.put(self.workflow_exception)
        with self.assertRaisesRegex(WorkflowException, 'workflow exception'):
            self.executor.raise_if_exception_queued({}, self.logger)
        self.assertTrue(mock_wait.called)

    @patch('calrissian.executor.wait')
    def test_raise_if_exception_queued_casts_to_workflow_exception(self, mock_wait):
        self.executor.exceptions.put(Exception('generic exception'))
        with self.assertRaisesRegex(WorkflowException, 'generic exception'):
            self.executor.raise_if_exception_queued({}, self.logger)
        self.assertTrue(mock_wait.called)

    @patch('calrissian.executor.wait')
    def test_raise_if_exception_queued_does_nothing_when_no_exceptions(self, mock_wait):
        self.assertTrue(self.executor.exceptions.empty())
        self.executor.raise_if_exception_queued({}, self.logger)
        self.assertFalse(mock_wait.called)  # wait should only be called to wait for outstanding futures

    @patch('calrissian.executor.wait')
    def test_raise_if_exception_queued_cancels_futures(self, mock_wait):
        future = Future()
        self.assertFalse(future.cancelled())  # not initially cancelled
        self.executor.exceptions.put(self.workflow_exception)
        with self.assertRaisesRegex(WorkflowException, 'workflow exception'):
            self.executor.raise_if_exception_queued({future}, self.logger)
        self.assertTrue(mock_wait.called)
        self.assertTrue(future.cancelled())  # cancelled after exception called

    @patch('calrissian.executor.wait')
    def test_raise_if_exception_queued_handles_multiple(self, mock_wait):
        self.executor.exceptions.put(Exception('ex1'))
        self.executor.exceptions.put(Exception('ex2'))
        with self.assertRaisesRegex(WorkflowException, "ex1.*ex2"):
            self.executor.raise_if_exception_queued({}, self.logger)
        self.assertTrue(mock_wait.called)

    def test_raise_if_oversized_raises_with_oversized(self):
        rsc = Resources(100, 4)
        self.assertTrue(rsc.exceeds(self.executor.total_resources))
        job = make_mock_job(rsc)
        with self.assertRaisesRegex(OversizedJobException, 'exceed total'):
            self.executor.raise_if_oversized(job)

    def test_raise_if_oversized_does_nothing(self):
        rsc = Resources(100, 1)
        self.assertFalse(rsc.exceeds(self.executor.total_resources))
        job = make_mock_job(rsc)
        self.executor.raise_if_oversized(job)

    @patch('calrissian.executor.ThreadPoolJobExecutor.restore')
    def test_job_done_callback_extracts_future_exception(self, mock_restore):
        future = Future()
        future.set_exception(self.workflow_exception)
        rsc = Mock()
        self.executor.job_done_callback(rsc, self.logger, future)
        self.assertEqual(self.workflow_exception, self.executor.exceptions.get())
        self.assertEqual(mock_restore.call_args, call(rsc, self.logger))

    @patch('calrissian.executor.ThreadPoolJobExecutor.restore')
    def test_job_done_callback_bails_out_if_canceled(self, mock_restore):
        future = Future()
        future.cancel()
        self.assertTrue(future.cancelled())
        rsc = Mock()
        self.executor.job_done_callback(rsc, self.logger, future)
        self.assertEqual(mock_restore.call_args, call(rsc, self.logger))
        self.assertTrue(self.executor.exceptions.empty())

    @patch('calrissian.executor.ThreadPoolJobExecutor.restore')
    def test_job_done_callback_catches_restore_exception(self, mock_restore):
        exception = InconsistentResourcesException('inconsistent resources')
        mock_restore.side_effect = exception
        self.executor.job_done_callback(Resources(1, 1), self.logger, Mock())
        self.assertEqual(exception, self.executor.exceptions.get())

    @patch('calrissian.executor.JobResourceQueue.dequeue')
    @patch('calrissian.executor.ThreadPoolJobExecutor.allocate')
    def test_start_queued_jobs(self, mock_allocate, mock_dequeue):
        job_resources = [Resources(100, 1), Resources(200, 2)]
        mock_runnable_jobs = {make_mock_job(r): r for r in job_resources}
        mock_dequeue.return_value = mock_runnable_jobs
        pool_executor = Mock()
        mock_future = Mock()
        pool_executor.submit.return_value = mock_future
        mock_runtime_context = Mock(builder=Mock())
        result = self.executor.start_queued_jobs(pool_executor, self.logger, mock_runtime_context)
        self.assertEqual(mock_dequeue.call_args, call(self.executor.available_resources))

        # allocates resources
        self.assertEqual(mock_allocate.call_args_list, [
            call(job_resources[0], self.logger),
            call(job_resources[1], self.logger)
        ])
        # connects builder and output_dirs
        self.assertTrue(all([j.builder == mock_runtime_context.builder for j in mock_runnable_jobs]))
        self.assertEqual(self.executor.output_dirs, {j.outdir for j in mock_runnable_jobs})

        # submits a future
        self.assertEqual(pool_executor.submit.call_args_list, [
            call(j.run, mock_runtime_context) for j in mock_runnable_jobs
        ])
        # returns set of submitted futures
        self.assertIn(mock_future, result)

    @patch('calrissian.executor.wait')
    @patch('calrissian.executor.FIRST_COMPLETED')
    def test_wait_for_completion(self, mock_first_completed, mock_wait):
        mock_futures = {Mock()}
        result = self.executor.wait_for_completion(mock_futures, self.logger)
        self.assertEqual(result, mock_wait.return_value.not_done)
        self.assertEqual(mock_wait.call_args, call(mock_futures, return_when=mock_first_completed))


class ThreadPoolJobExecutorQueueingTestCase(ThreadPoolJobExecutorTestCase):

    def setUp(self):
        super(ThreadPoolJobExecutorQueueingTestCase, self).setUp()
        self.jobs_simple = [
            make_mock_job(Resources(100, 1)),
            make_mock_job(Resources(200, 2)),
            make_mock_job(Resources(150, 3))
        ]
        self.jobs_with_none = [
            make_mock_job(Resources(100, 1)),
            make_mock_job(Resources(200, 2)),
            None,
            make_mock_job(Resources(150, 3))
        ]
        self.mock_pool_executor = create_autospec(ThreadPoolExecutor)
        self.mock_runtime_context = Mock(
            workflow_eval_lock=threading.Condition(threading.RLock())
        )

    @staticmethod
    def iterator(iterable):
        for j in iterable:
            yield j

    @patch('calrissian.executor.JobResourceQueue.enqueue')
    @patch('calrissian.executor.ThreadPoolJobExecutor.raise_if_oversized')
    @patch('calrissian.executor.ThreadPoolJobExecutor.start_queued_jobs')
    def test_enqueues_jobs_simple(self, mock_start_queued_jobs, mock_raise_if_oversized, mock_enqueue):
        manager = Mock()
        manager.attach_mock(mock_enqueue, 'enqueue')
        manager.attach_mock(mock_start_queued_jobs, 'start_queued_jobs')
        expected_calls = [
            call.enqueue(self.jobs_simple[0]),
            call.enqueue(self.jobs_simple[1]),
            call.enqueue(self.jobs_simple[2])
        ]
        self.assertNotIn(None, self.jobs_simple)
        iterator = self.iterator(self.jobs_simple)

        # When none of the jobs provided are None, this method should only queue jobs and not start running anything
        result = self.executor.enqueue_jobs_from_iterator(iterator, self.logger, self.mock_runtime_context,
                                                          self.mock_pool_executor)
        self.assertEqual(expected_calls, manager.mock_calls)
        self.assertEqual(result, set())  # Nothing submitted, futures should be an empty set

    @patch('calrissian.executor.JobResourceQueue.enqueue')
    @patch('calrissian.executor.ThreadPoolJobExecutor.raise_if_oversized')
    @patch('calrissian.executor.ThreadPoolJobExecutor.start_queued_jobs')
    @patch('calrissian.executor.ThreadPoolJobExecutor.wait_for_completion')
    def test_enqueues_jobs_with_none(self, mock_wait_for_completion, mock_start_queued_jobs,
                                     mock_raise_if_oversized, mock_enqueue):
        mock_waited_futures = Mock()
        mock_wait_for_completion.return_value = mock_waited_futures
        manager = Mock()
        manager.attach_mock(mock_enqueue, 'enqueue')
        manager.attach_mock(mock_start_queued_jobs, 'start_queued_jobs')
        expected_calls = [
            call.enqueue(self.jobs_with_none[0]),
            call.enqueue(self.jobs_with_none[1]),
            call.start_queued_jobs(self.mock_pool_executor, self.logger, self.mock_runtime_context),
            call.enqueue(self.jobs_with_none[3])
        ]

        self.assertIn(None, self.jobs_with_none)
        iterator = self.iterator(self.jobs_with_none)
        result = self.executor.enqueue_jobs_from_iterator(iterator, self.logger, self.mock_runtime_context,
                                                          self.mock_pool_executor)
        self.assertEqual(expected_calls, manager.mock_calls)

        # All jobs from the iterator should be queued
        self.assertEqual(mock_enqueue.call_args_list, [call(j) for j in self.jobs_with_none if j])
        self.assertEqual(result, mock_waited_futures)

    @patch('calrissian.executor.JobResourceQueue.is_empty')
    @patch('calrissian.executor.ThreadPoolJobExecutor.start_queued_jobs')
    @patch('calrissian.executor.ThreadPoolJobExecutor.wait_for_completion')
    def test_drain_queue(self, mock_wait_for_completion, mock_start_queued_jobs, mock_is_empty):
        mock_is_empty.return_value = True
        initial_futures = {'initial'}
        submitted_futures = {'submitted'}
        waiting_futures = set()
        mock_start_queued_jobs.return_value = submitted_futures
        mock_wait_for_completion.return_value = waiting_futures  # Empty set

        self.executor.drain_queue(self.logger, self.mock_runtime_context, self.mock_pool_executor, initial_futures)
        # calls start_queued_jobs and updates the provided futures with its result
        self.assertEqual(mock_start_queued_jobs.call_args,
                         call(self.mock_pool_executor, self.logger, self.mock_runtime_context))

        # calls wait for completion with the whole set of futures
        self.assertEqual(mock_wait_for_completion.call_args, call({'initial', 'submitted'}, self.logger))
        self.assertTrue(mock_is_empty.called)

    def test_run_jobs_raises_if_no_lock(self):
        mock_process = Mock()
        mock_job_order = Mock()
        mock_runtime_context = Mock(workflow_eval_lock=None)
        with self.assertRaisesRegex(WorkflowException, 'workflow_eval_lock must not be None'):
            self.executor.run_jobs(mock_process, mock_job_order, self.logger, mock_runtime_context)

    @patch('calrissian.executor.ThreadPoolExecutor', autospec=True)
    @patch('calrissian.executor.ThreadPoolJobExecutor.enqueue_jobs_from_iterator')
    @patch('calrissian.executor.ThreadPoolJobExecutor.drain_queue')
    def test_run_jobs(self, mock_drain_queue, mock_enqueue_jobs, mock_executor):
        mock_context_executor = mock_executor.return_value.__enter__.return_value
        mock_job_order = Mock()
        mock_process = Mock()
        mock_job_iterator = Mock()
        mock_process.job.return_value = mock_job_iterator
        mock_enqueued_futures = Mock()
        mock_enqueue_jobs.return_value = mock_enqueued_futures
        self.executor.run_jobs(mock_process, mock_job_order, self.logger, self.mock_runtime_context)
        self.assertEqual(mock_enqueue_jobs.call_args,
                         call(mock_job_iterator, self.logger, self.mock_runtime_context, mock_context_executor))
        self.assertEqual(mock_drain_queue.call_args,
                         call(self.logger, self.mock_runtime_context, mock_context_executor, mock_enqueued_futures))
