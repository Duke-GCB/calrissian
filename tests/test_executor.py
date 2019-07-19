from unittest import TestCase
from calrissian.executor import CalrissianExecutor
from unittest.mock import patch, call


class CalrissianExecutorTestCase(TestCase):

    def test_init(self):
        e = CalrissianExecutor(0, 0)
        self.assertIsNotNone(e)

    def test_max_ram(self):
        e = CalrissianExecutor(100, 0)
        self.assertEqual(e.max_ram, 100)
        # Test that base class init is still called
        self.assertEqual(e.allocated_ram, 0)
        self.assertEqual(e.allocated_cores, 0)

    def test_max_cores(self):
        e = CalrissianExecutor(0, 4)
        self.assertEqual(e.max_cores, 4)
        # Test that base class init is still called
        self.assertEqual(e.allocated_ram, 0)
        self.assertEqual(e.allocated_cores, 0)

    @patch('calrissian.executor.log')
    def test_report_usage(self, mock_log):
        e = CalrissianExecutor(100, 1)
        e.report_usage('test')
        self.assertEqual(mock_log.debug.call_args, call('test: Allocated 0/1 CPU cores, 0/100 MB RAM'))


@patch('calrissian.executor.CalrissianExecutor.report_usage')
class CalrisssianExecutorOverridesTestCase(TestCase):

    def setUp(self):
        self.e = CalrissianExecutor(0, 0)

    @patch('calrissian.executor.MultithreadedJobExecutor.run_job')
    def test_run_job(self, mock_run_job, mock_report_usage):
        self.e.run_job()
        self.assertEqual(mock_report_usage.call_args, call('run_job'))
        self.assertTrue(mock_run_job.called)

    @patch('calrissian.executor.MultithreadedJobExecutor.wait_for_next_completion')
    def test_wait_for_next_completion(self, mock_wait_for_next_completion, mock_report_usage):
        self.e.wait_for_next_completion()
        self.assertEqual(mock_report_usage.call_args, call('wait_for_next_completion'))
        self.assertTrue(mock_wait_for_next_completion.called)

    @patch('calrissian.executor.MultithreadedJobExecutor.run_jobs')
    def test_run_jobs(self, mock_run_jobs, mock_report_usage):
        self.e.run_jobs()
        self.assertEqual(mock_report_usage.call_args, call('run_jobs'))
        self.assertTrue(mock_run_jobs.called)

    @patch('calrissian.executor.MultithreadedJobExecutor.execute')
    def test_execute(self, mock_execute, mock_report_usage):
        self.e.execute()
        self.assertEqual(mock_report_usage.call_args, call('execute'))
        self.assertTrue(mock_execute.called)
