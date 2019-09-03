from unittest import TestCase
from calrissian.thread_pool_executor import *
from unittest.mock import patch, call, Mock


class ResourcesTestCase(TestCase):

    def setUp(self):
        self.resource11 = Resources(1,1)
        self.resource22 = Resources(2,2)
        self.resource33 = Resources(3,3)
        self.resource21 = Resources(2,1)

    def test_init(self):
        self.assertEqual(self.resource11.cpu, 1)
        self.assertEqual(self.resource11.ram, 1)

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
        mock_job = Mock(builder=Mock(resources={'ram': 4, 'cpu': 2}))
        result = Resources.from_job(mock_job)
        self.assertEqual(result.ram, 4)
        self.assertEqual(result.cpu, 2)

    def test_empty(self):
        self.assertEqual(Resources.EMPTY.ram, 0)
        self.assertEqual(Resources.EMPTY.cpu, 0)


class JobResourceQueueTestCase(TestCase):

    pass

