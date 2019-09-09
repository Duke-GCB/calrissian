from unittest import TestCase
from calrissian.thread_pool_executor import *
from unittest.mock import patch, call, Mock


def make_mock_job(ram, cpu):
    return Mock(builder=Mock(resources={'ram': ram, 'cpu': cpu}))


class ResourcesTestCase(TestCase):

    def setUp(self):
        self.resource11 = Resources(1,1)
        self.resource22 = Resources(2,2)
        self.resource33 = Resources(3,3)
        self.resource21 = Resources(2,1)
        self.resource_neg = Resources(-1,0)

    def test_init(self):
        self.assertEqual(self.resource11.cpu, 1)
        self.assertEqual(self.resource11.ram, 1)
        self.assertEqual(self.resource22.cpu, 2)
        self.assertEqual(self.resource22.ram, 2)
        self.assertEqual(self.resource33.cpu, 3)
        self.assertEqual(self.resource33.ram, 3)
        self.assertEqual(self.resource21.cpu, 1)
        self.assertEqual(self.resource21.ram, 2)

    def test_subtraction(self):
        result = self.resource33 - self.resource21
        self.assertEqual(result.ram, 1)
        self.assertEqual(result.cpu, 2)

    def test_addition(self):
        result = self.resource11 + self.resource22
        self.assertEqual(result.ram, 3)
        self.assertEqual(result.cpu, 3)

    def test_neg(self):
        result = - self.resource11
        self.assertEqual(result.ram, -1)
        self.assertEqual(result.cpu, -1)

    def test_lt(self):
        self.assertTrue(self.resource11 < self.resource22)
        self.assertTrue(self.resource21 < self.resource33)
        self.assertFalse(self.resource11 < self.resource21)

    def test_gt(self):
        self.assertTrue(self.resource22 > self.resource11)
        self.assertTrue(self.resource33 > self.resource21)
        self.assertFalse(self.resource21 > self.resource11)

    def test_eq(self):
        other = Resources(1,1)
        self.assertEqual(self.resource11, other)

    def test_from_job(self):
        mock_job = make_mock_job(4, 2)
        result = Resources.from_job(mock_job)
        self.assertEqual(result.ram, 4)
        self.assertEqual(result.cpu, 2)

    def test_is_negative(self):
        self.assertFalse(self.resource11.is_negative())
        self.assertTrue(self.resource_neg.is_negative())

    def test_empty(self):
        self.assertEqual(Resources.EMPTY.ram, 0)
        self.assertEqual(Resources.EMPTY.cpu, 0)


class JobResourceQueueTestCase(TestCase):

    def setUp(self):
        self.jrq = JobResourceQueue()
        self.job100_4 = make_mock_job(100, 4)   # 100 RAM, 2 CPU
        self.job200_2 = make_mock_job(200, 2)   # 200 RAM, 4 CPU
        self.jobs = {self.job100_4: Resources(100, 4), self.job200_2: Resources(200,2)}

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

    def test_dequeue_none_fit_cpu_too_small(self):
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

    def test_smallest_cpu_first(self):
        self.jrq.priority = Resources.CPU
        self.jrq.descending = False
        limit = Resources(250, 5)
        self.queue_jobs()
        runnable = self.jrq.dequeue(limit)
        self.assertEqual(len(runnable), 1)
        self.assertIn(self.job200_2, runnable)

    def test_pop_runnable_job_largest_cpu_first(self):
        self.jrq.priority = Resources.CPU
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
