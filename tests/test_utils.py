"""Test utilities and decorators."""

import asyncio
import functools
from typing import Any, Awaitable, Callable


def async_timeout(timeout_seconds: float = 10.0):
    """Decorator to add timeout to async test functions.

    Args:
        timeout_seconds: Maximum time to allow the test to run

    Usage:
        @async_timeout(5.0)
        async def test_something():
            # Test code here
    """

    def decorator(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                async with asyncio.timeout(timeout_seconds):
                    return await func(*args, **kwargs)
            except asyncio.TimeoutError:
                raise AssertionError(
                    f"Test {func.__name__} timed out after {timeout_seconds}s"
                )

        return wrapper

    return decorator
