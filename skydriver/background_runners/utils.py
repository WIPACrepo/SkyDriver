"""Common utilities."""

import asyncio
import functools
import logging


def resilient_loop(display_name: str, delay_seconds: int, logger: logging.Logger):
    """Decorator to restart an async function on failure after delay.

    Waits hopefully long enough that any transient errors are resolved;
    e.g. a mongo pod failure and restart (otherwise, tries again).
    """

    def decorator(func):

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            logger.info(f"Started {display_name}.")
            while True:
                try:
                    await func(*args, **kwargs)
                except Exception as e:
                    logger.exception(e)
                    logger.error(
                        f"above error stopped {display_name}, "
                        f"resuming in {delay_seconds} seconds..."
                    )
                await asyncio.sleep(delay_seconds)
                logger.info(f"Restarted {display_name}.")

        return wrapper

    return decorator
