import os
from contextlib import contextmanager
from typing import Optional


@contextmanager
def env_vars(**kwargs: Optional[str]):
    """
    A context manager that temporarly changes environment variables. The call arguments their names are the names of
    the environment variables and the values are their value. As such the values are expected to be string if provided.
    A value of None signifies that the environment value must not exist.
    """
    backup_existing_environment: dict[str, Optional[str]] = {}
    for k, v in kwargs.items():
        assert v is None or isinstance(v, str), "Environment values must be of type str or set to None"
    try:
        for k, v in kwargs.items():
            backup_existing_environment[k] = os.environ.get(k)
            if v is None:
                if k in os.environ:
                    del os.environ[k]
            else:
                os.environ[k] = v
        yield
    finally:
        # Restore the original values
        for k, v in backup_existing_environment.items():
            if v is None:
                if k in os.environ:
                    del os.environ[k]
            else:
                os.environ[k] = v
