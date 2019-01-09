from unittest import TestCase, mock
from calrissian.executor import CalrissianExecutor


class CalrissianExecutorTestCase(TestCase):

    def test_init(self):
        e = CalrissianExecutor()
        self.assertIsNotNone(e)
