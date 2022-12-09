"""Utilities."""

import inspect
from functools import wraps
from typing import Callable, ParamSpec, TypeVar

# https://stackoverflow.com/a/65681776
# https://stackoverflow.com/a/71324646
T = TypeVar("T")  # the callable/awaitable return type
P = ParamSpec("P")  # the callable parameters


def get_function_as_str(func: Callable[P, T], args, kwargs) -> str:
    """Get the function name with signature and values.

    Ex: funk(a=1, b=100, c=2)
    """
    bound_args = inspect.signature(func).bind(*args, **kwargs)
    bound_args.apply_defaults()

    return f"{func.__qualname__}({', '.join(f'{k}={v}' for k, v in dict(bound_args.arguments).items())})"


def log_and_call(logger, statement):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # set name_override to func.__name__
            logger.info(f"{get_function_as_str(func, args, kwargs)}: {statement}")
            return func(*args, **kwargs)

        return wrapper

    return decorator
