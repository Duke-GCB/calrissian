from calrissian.retry import retry_exponential_if_exception_type
from unittest import TestCase
from unittest.mock import Mock, patch


class RetryTestCase(TestCase):
    def setUp(self):
        self.logger = Mock()
        self.mock = Mock()

    def setup_mock_retry_parameters(self, mock_retry_parameters):
        mock_retry_parameters.MULTIPLIER = 0.001
        mock_retry_parameters.MIN = 0.001
        mock_retry_parameters.MAX = 0.010
        mock_retry_parameters.ATTEMPTS = 5

    def test_retry_calls_wrapped_function(self):
        @retry_exponential_if_exception_type(ValueError, self.logger)
        def func():
            return self.mock()

        result = func()
        self.assertEqual(result, self.mock.return_value)
        self.assertEqual(self.mock.call_count, 1)

    @patch('calrissian.retry.RetryParameters')
    def test_retry_gives_up_and_raises(self, mock_retry_parameters):
        self.setup_mock_retry_parameters(mock_retry_parameters)
        self.mock.side_effect = ValueError('value error')

        @retry_exponential_if_exception_type(ValueError, self.logger)
        def func():
            self.mock()

        with self.assertRaisesRegex(ValueError, 'value error'):
            func()

        self.assertEqual(self.mock.call_count, 5)

    @patch('calrissian.retry.RetryParameters')
    def test_retry_eventually_succeeds_without_exception(self, mock_retry_parameters):
        self.setup_mock_retry_parameters(mock_retry_parameters)

        @retry_exponential_if_exception_type(ValueError, self.logger)
        def func():
            r = self.mock()
            if self.mock.call_count < 3:
                raise ValueError('value error')
            return r

        result = func()

        self.assertEqual(result, self.mock.return_value)
        self.assertEqual(self.mock.call_count, 3)

    @patch('calrissian.retry.RetryParameters')
    def test_retry_raises_other_exceptions_without_second_attempt(self, mock_retry_parameters):
        self.setup_mock_retry_parameters(mock_retry_parameters)

        class ExceptionA(Exception): pass
        class ExceptionB(Exception): pass

        self.mock.side_effect = ExceptionA('exception a')

        @retry_exponential_if_exception_type(ExceptionB, self.logger)
        def func():
            self.mock()

        with self.assertRaisesRegex(ExceptionA, 'exception a'):
            func()

        self.assertEqual(self.mock.call_count, 1)
