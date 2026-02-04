from calrissian.retry import retry_exponential_if_exception_type
from unittest import TestCase
from unittest.mock import Mock, patch

class FakeApiException(Exception):
    def __init__(self, status, reason=""):
        super().__init__(reason or f"HTTP {status}")
        self.status = status
        self.reason = reason or f"HTTP {status}"

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


    @patch("calrissian.retry.RetryParameters")
    def test_no_retry_on_4xx_immediate_raise(self, mock_retry_parameters):
        """403 should NOT retry; call count stays 1."""
        self.setup_mock_retry_parameters(mock_retry_parameters)
        self.mock.side_effect = FakeApiException(403, "Forbidden")

        @retry_exponential_if_exception_type(FakeApiException, self.logger)
        def wrapped():
            return self.mock()

        with self.assertRaisesRegex(FakeApiException, "Forbidden"):
            wrapped()

        self.assertEqual(self.mock.call_count, 1)  # no retries


    @patch("calrissian.retry.RetryParameters")
    def test_retry_on_5xx_then_give_up(self, mock_retry_parameters):
        """500 should retry up to ATTEMPTS and then raise."""
        self.setup_mock_retry_parameters(mock_retry_parameters)
        self.mock.side_effect = FakeApiException(500, "Internal Error")

        @retry_exponential_if_exception_type(FakeApiException, self.logger)
        def wrapped():
            return self.mock()

        with self.assertRaisesRegex(FakeApiException, "Internal Error"):
            wrapped()

        self.assertEqual(self.mock.call_count, mock_retry_parameters.ATTEMPTS)


    @patch("calrissian.retry.RetryParameters")
    def test_retry_on_5xx_eventually_succeeds(self, mock_retry_parameters):
        """Fail twice with 500, succeed on third call."""
        self.setup_mock_retry_parameters(mock_retry_parameters)
        self.mock.side_effect = [
            FakeApiException(500, "Internal Error"),
            FakeApiException(500, "Internal Error"),
            "ok",
        ]

        @retry_exponential_if_exception_type(FakeApiException, self.logger)
        def wrapped():
            return self.mock()

        result = wrapped()
        self.assertEqual(result, "ok")
        self.assertEqual(self.mock.call_count, 3)


    @patch("calrissian.retry.RetryParameters")
    def test_retry_when_exception_has_no_status_attribute(self, mock_retry_parameters):
        """Edge case: exception has no 'status' attribute"""
        self.setup_mock_retry_parameters(mock_retry_parameters)
        class NoStatusException(Exception):
            pass

        self.mock.side_effect = NoStatusException("no_status")

        @retry_exponential_if_exception_type(NoStatusException, self.logger)
        def wrapped():
            return self.mock()

        with self.assertRaisesRegex(NoStatusException, "no_status"):
            wrapped()

        self.assertEqual(self.mock.call_count, mock_retry_parameters.ATTEMPTS)


    @patch("calrissian.retry.RetryParameters")
    def test_retry_when_exception_status_is_non_numeric_string(self, mock_retry_parameters):
        """Edge case: status is present but not parseable (ValueError in int())"""
        self.setup_mock_retry_parameters(mock_retry_parameters)

        class WeirdStatusException(Exception):
            def __init__(self, status, reason="weird"):
                super().__init__(reason)
                self.status = status

        self.mock.side_effect = WeirdStatusException("four hundred")

        @retry_exponential_if_exception_type(WeirdStatusException, self.logger)
        def wrapped():
            return self.mock()

        with self.assertRaisesRegex(WeirdStatusException, "weird"):
            wrapped()

        self.assertEqual(self.mock.call_count, mock_retry_parameters.ATTEMPTS)


    @patch("calrissian.retry.RetryParameters")
    def test_retry_when_exception_status_is_none(self, mock_retry_parameters):
        """Edge case: status is None (TypeError in int())"""
        self.setup_mock_retry_parameters(mock_retry_parameters)
        class NoneStatusException(Exception):
            def __init__(self, reason="none_status"):
                super().__init__(reason)
                self.status = None

        self.mock.side_effect = NoneStatusException("none_status")

        @retry_exponential_if_exception_type(NoneStatusException, self.logger)
        def wrapped():
            return self.mock()

        with self.assertRaisesRegex(NoneStatusException, "none_status"):
            wrapped()

        self.assertEqual(self.mock.call_count, mock_retry_parameters.ATTEMPTS)


    @patch("calrissian.retry.RetryParameters")
    def test_retry_boundary_399_retries_then_give_up(self, mock_retry_parameters):
        """Boundary: 399 is NOT 4xx -> should retry."""
        self.setup_mock_retry_parameters(mock_retry_parameters)
        self.mock.side_effect = FakeApiException(399, "HTTP 399")

        @retry_exponential_if_exception_type(FakeApiException, self.logger)
        def wrapped():
            return self.mock()

        with self.assertRaisesRegex(FakeApiException, "HTTP 399"):
            wrapped()

        self.assertEqual(self.mock.call_count, mock_retry_parameters.ATTEMPTS)


    @patch("calrissian.retry.RetryParameters")
    def test_retry_boundary_400_no_retry(self, mock_retry_parameters):
        """Boundary: 400 IS 4xx -> should NOT retry."""
        self.setup_mock_retry_parameters(mock_retry_parameters)
        self.mock.side_effect = FakeApiException(400, "Bad Request")

        @retry_exponential_if_exception_type(FakeApiException, self.logger)
        def wrapped():
            return self.mock()

        with self.assertRaisesRegex(FakeApiException, "Bad Request"):
            wrapped()

        self.assertEqual(self.mock.call_count, 1)


    @patch("calrissian.retry.RetryParameters")
    def test_retry_boundary_499_no_retry(self, mock_retry_parameters):
        """Boundary: 499 IS 4xx -> should NOT retry."""
        self.setup_mock_retry_parameters(mock_retry_parameters)
        self.mock.side_effect = FakeApiException(499, "HTTP 499")

        @retry_exponential_if_exception_type(FakeApiException, self.logger)
        def wrapped():
            return self.mock()

        with self.assertRaisesRegex(FakeApiException, "HTTP 499"):
            wrapped()

        self.assertEqual(self.mock.call_count, 1)


    @patch("calrissian.retry.RetryParameters")
    def test_retry_boundary_500_retries_then_give_up(self, mock_retry_parameters):
        """Boundary: 500 is NOT 4xx -> should retry."""
        self.setup_mock_retry_parameters(mock_retry_parameters)
        self.mock.side_effect = FakeApiException(500, "Internal Error")

        @retry_exponential_if_exception_type(FakeApiException, self.logger)
        def wrapped():
            return self.mock()

        with self.assertRaisesRegex(FakeApiException, "Internal Error"):
            wrapped()

        self.assertEqual(self.mock.call_count, mock_retry_parameters.ATTEMPTS)


    @patch("calrissian.retry.RetryParameters")
    def test_retry_on_410_gone_error(self, mock_retry_parameters):
        """410 (Gone) IS 4xx but SHOULD retry due to special handling."""
        self.setup_mock_retry_parameters(mock_retry_parameters)
        self.mock.side_effect = FakeApiException(410, "Expired: too old resource version")

        @retry_exponential_if_exception_type(FakeApiException, self.logger)
        def wrapped():
            return self.mock()

        with self.assertRaisesRegex(FakeApiException, "Expired"):
            wrapped()

        # Should retry because 410 is a retryable 4xx
        self.assertEqual(self.mock.call_count, mock_retry_parameters.ATTEMPTS)


    @patch("calrissian.retry.RetryParameters")
    def test_retry_on_410_eventually_succeeds(self, mock_retry_parameters):
        """410 should retry and eventually succeed."""
        self.setup_mock_retry_parameters(mock_retry_parameters)
        self.mock.side_effect = [
            FakeApiException(410, "Expired: too old resource version"),
            FakeApiException(410, "Expired: too old resource version"),
            "ok",
        ]

        @retry_exponential_if_exception_type(FakeApiException, self.logger)
        def wrapped():
            return self.mock()

        result = wrapped()
        self.assertEqual(result, "ok")
        self.assertEqual(self.mock.call_count, 3)
