from tenacity import retry, wait_exponential, retry_if_exception, retry_if_exception_type, stop_after_attempt, before_sleep_log
import logging
import os


class RetryParameters(object):
    MULTIPLIER = float(os.getenv('RETRY_MULTIPLIER', 5)) #  Unit for multiplying the exponent
    MIN = float(os.getenv('RETRY_MIN', 5)) # Min time for retrying
    MAX = float(os.getenv('RETRY_MAX', 1200)) # Max interval between retries
    ATTEMPTS = int(os.getenv('RETRY_ATTEMPTS', 10)) # Max number of retries before giving up


def _is_4xx(exc) -> bool:
    status = getattr(exc, "status", None)
    try:
        return 400 <= int(status) < 500
    except (TypeError, ValueError):
        return False


def _is_retryable_4xx(exc) -> bool:
    """
    Check if a 4xx error is retryable.
    410 (Gone) errors are retryable because they indicate the watch resource version is too old.
    """
    status = getattr(exc, "status", None)
    try:
        return int(status) == 410
    except (TypeError, ValueError):
        return False


def retry_exponential_if_exception_type(exc_type, logger):
    """
    Decorator function that returns the tenacity @retry decorator with our commonly-used config
    :param exc_type: Type of exception (or tuple of types) to retry if encountered
    :param logger: A logger instance to send retry logs to
    :return: Result of tenacity.retry decorator function
    """
    retry_on_type = retry_if_exception_type(exc_type)
    retry_not_4xx = retry_if_exception(lambda e: not _is_4xx(e))
    retry_on_410 = retry_if_exception(_is_retryable_4xx)

    return retry(retry=retry_on_type & (retry_not_4xx | retry_on_410),
            wait=wait_exponential(multiplier=RetryParameters.MULTIPLIER, min=RetryParameters.MIN, max=RetryParameters.MAX),
            stop=stop_after_attempt(RetryParameters.ATTEMPTS),
            before_sleep=before_sleep_log(logger, logging.DEBUG),
            reraise=True)
