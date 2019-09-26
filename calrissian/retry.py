from tenacity import retry, wait, wait_exponential, retry_unless_exception_type
import os
import functools

class WaitRetry(object):
    MULTIPLIER = int(os.getenv('WAIT_RETRY_MULTIPLIER', 1))
    MIN = int(os.getenv('WAIT_RETRY_MIN', 4))
    MAX = int(os.getenv('WAIT_RETRY_MAX', 10))


def retry_exponential_unless_exception_type(exc_class):
    def decorator_retry(func):
        @retry(
            retry=retry_unless_exception_type(exc_class),
            wait=wait_exponential(multiplier=WaitRetry.MULTIPLIER, min=WaitRetry.MIN, max=WaitRetry.MAX)
        )
        def wrapper(*args, **kwargs):
            func(*args, **kwargs)
        return wrapper
    return decorator_retry

