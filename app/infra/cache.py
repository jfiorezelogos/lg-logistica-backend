# app/infra/cache.py
from collections.abc import Callable
from functools import lru_cache
from typing import TypeVar

F = TypeVar("F", bound=Callable[..., object])


def simple_cache(maxsize: int = 1) -> Callable[[F], F]:
    """
    Wrapper em torno de functools.lru_cache.
    - Permite trocar facilmente a implementação de cache no futuro
      (ex.: Redis, Memcached, in-memory custom).
    - Usa maxsize=1 por padrão para loaders de arquivos de configuração
      que raramente mudam.
    """
    return lru_cache(maxsize=maxsize)
