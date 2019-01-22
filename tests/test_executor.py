from unittest import TestCase
from calrissian.executor import CalrissianExecutor


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
