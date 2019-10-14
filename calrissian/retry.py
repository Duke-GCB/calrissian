from tenacity import retry, wait_exponential, retry_if_exception_type, stop_after_attempt, before_sleep_log
import logging
import os


class RetryParameters(object):
    MULTIPLIER = float(os.getenv('RETRY_MULTIPLIER', 5)) #  Unit for multiplying the exponent
    MIN = float(os.getenv('RETRY_MIN', 5)) # Min time for retrying
    MAX = float(os.getenv('RETRY_MAX', 1200)) # Max interval between retries
    ATTEMPTS = int(os.getenv('RETRY_ATTEMPTS', 10)) # Max number of retries before giving up


def retry_exponential_if_exception_type(exc_type, logger):
    """
    Decorator function that returns the tenacity @retry decorator with our commonly-used config
    :param exc_type: Type of exception (or tuple of types) to retry if encountered
    :param logger: A logger instance to send retry logs to
    :return: Result of tenacity.retry decorator function
    """
    return retry(retry=retry_if_exception_type(exc_type),
            wait=wait_exponential(multiplier=RetryParameters.MULTIPLIER, min=RetryParameters.MIN, max=RetryParameters.MAX),
            stop=stop_after_attempt(RetryParameters.ATTEMPTS),
            before_sleep=before_sleep_log(logger, logging.DEBUG),
            reraise=True)
