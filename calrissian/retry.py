from tenacity import retry, wait, wait_exponential, retry_if_exception_type, stop_after_attempt, before_sleep_log
import logging
import os


logger = logging.getLogger('calrissian.retry')


class RetryParameters(object):
    MULTIPLIER = int(os.getenv('RETRY_MULTIPLIER', 5)) #  Unit for multiplying the exponent
    MIN = int(os.getenv('RETRY_MIN', 5)) # Min time for retrying
    MAX = int(os.getenv('RETRY_MAX', 1200)) # Max interval between retries
    ATTEMPTS = int(os.getenv('RETRY_ATTEMPTS', 10)) # Max number of retries before giving up


# types can be a tuple
def retry_exponential_if_exception_type(exc_class):
    def decorator_retry(func):
        @retry(
            retry=retry_if_exception_type(exc_class),
            wait=wait_exponential(multiplier=RetryParameters.MULTIPLIER, min=RetryParameters.MIN, max=RetryParameters.MAX),
            stop=stop_after_attempt(RetryParameters.ATTEMPTS),
            before_sleep=before_sleep_log(logger, logging.DEBUG),
            reraise=True
        )
        def wrapper(*args, **kwargs):
            func(*args, **kwargs)
        return wrapper
    return decorator_retry

