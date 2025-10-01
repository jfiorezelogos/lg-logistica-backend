from collections.abc import Callable
from functools import lru_cache
from typing import TypeVar, ParamSpec

P = ParamSpec("P")
R = TypeVar("R")

def simple_cache(maxsize: int = 1) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Wrapper em torno de functools.lru_cache.
    """
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        return lru_cache(maxsize=maxsize)(func)
    return decorator
