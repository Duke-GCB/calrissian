from tenacity import retry, wait, wait_exponential, retry_if_exception_type, stop_after_attempt, before_sleep_log
import logging
import os


logger = logging.getLogger('calrissian.retry')

class WaitRetry(object):
    MULTIPLIER = int(os.getenv('WAIT_RETRY_MULTIPLIER', 1)) # time unit for waiting between retries, e.g. 1 second
    MIN = int(os.getenv('WAIT_RETRY_MIN', 10)) # starting interval for retrying
    MAX = int(os.getenv('WAIT_RETRY_MAX', 60)) # max interval between retries
    STOP = int(os.getenv('WAIT_ATTEMPTS', 3)) # Max number of retries before giving up


# types can be a tuple
def retry_exponential_if_exception_type(exc_class):
    def decorator_retry(func):
        @retry(
            retry=retry_if_exception_type(exc_class),
            wait=wait_exponential(multiplier=WaitRetry.MULTIPLIER, min=WaitRetry.MIN, max=WaitRetry.MAX),
            stop=stop_after_attempt(WaitRetry.STOP),
            before_sleep=before_sleep_log(logger, logging.DEBUG)
        )
        def wrapper(*args, **kwargs):
            func(*args, **kwargs)
        return wrapper
    return decorator_retry

