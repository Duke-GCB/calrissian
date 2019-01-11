from unittest import TestCase
from calrissian.executor import CalrissianExecutor


class CalrissianExecutorTestCase(TestCase):

    def test_init(self):
        e = CalrissianExecutor()
        self.assertIsNotNone(e)
