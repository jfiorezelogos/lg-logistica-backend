from __future__ import annotations

import random
import time
from functools import lru_cache
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .errors import ExternalError
from .logging_setup import get_correlation_id, get_logger

DEFAULT_TIMEOUT: tuple[int, int] = (5, 30)
TRANSIENT_STATUSES = (429, 500, 502, 503, 504)
IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "POST"})

logger = get_logger("http")


def _build_retry(
    total: int = 5,
    backoff_factor: float = 0.5,
    statuses: tuple[int, ...] = TRANSIENT_STATUSES,
    methods: frozenset[str] = IDEMPOTENT_METHODS,
) -> Retry:
    """Cria política de retry exponencial com respeito a Retry-After (429/503)."""
    common_kwargs: dict[str, Any] = {
        "total": total,
        "connect": total,
        "read": total,
        "status": total,
        "backoff_factor": backoff_factor,
        "status_forcelist": statuses,
        "raise_on_status": False,
        "respect_retry_after_header": True,
    }
    try:
        return Retry(allowed_methods=methods, **common_kwargs)
    except TypeError:
        return Retry(method_whitelist=methods, **common_kwargs)  # compat antiga


def _build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "lg-logistica-v2/HTTPClient (+https://example.invalid)",
            "Accept": "application/json, */*;q=0.1",
        }
    )
    retry = _build_retry()
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


@lru_cache(maxsize=1)
def _get_cached_session() -> requests.Session:
    return _build_session()


def get_session(session: requests.Session | None = None) -> requests.Session:
    return session or _get_cached_session()


def _request_with_handling(method: str, url: str, **kwargs: Any) -> requests.Response:
    timeout = kwargs.pop("timeout", DEFAULT_TIMEOUT)
    session: requests.Session = kwargs.pop("session", get_session())
    jitter_max: float = kwargs.pop("jitter_max", 0.0)

    if jitter_max and jitter_max > 0:
        time.sleep(random.uniform(0, jitter_max))

    # adiciona correlation-id
    headers = kwargs.pop("headers", {}) or {}
    headers = {**session.headers, **headers}
    headers.setdefault("X-Correlation-ID", get_correlation_id())
    kwargs["headers"] = headers

    try:
        res = session.request(method, url, timeout=timeout, **kwargs)
        res.raise_for_status()
        logger.info(
            "HTTP %s OK",
            method,
            extra={"url": url, "status": res.status_code, "cid": get_correlation_id()},
        )
        return res

    except requests.Timeout as e:
        logger.warning("HTTP %s timeout", method, extra={"url": url})
        raise ExternalError(
            f"Timeout ao chamar {url}",
            code="HTTP_TIMEOUT",
            cause=e,
            retryable=True,
            data={"url": url},
        ) from e

    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        retryable = bool(status in TRANSIENT_STATUSES)
        logger.error(
            "HTTP %s error",
            method,
            extra={
                "url": url,
                "status": status,
                "retryable": retryable,
                "cid": get_correlation_id(),
            },
        )
        raise ExternalError(
            f"Falha HTTP {status} ao chamar {url}",
            code="HTTP_ERROR",
            cause=e,
            retryable=retryable,
            data={
                "url": url,
                "status": status,
                "text": getattr(e.response, "text", None),
            },
        ) from e

    except requests.RequestException as e:
        logger.error(
            "HTTP %s request exception",
            method,
            extra={"url": url, "cid": get_correlation_id()},
        )
        raise ExternalError(
            f"Erro de rede ao chamar {url}",
            code="HTTP_REQUEST_ERROR",
            cause=e,
            retryable=True,
            data={"url": url},
        ) from e


def http_get(url: str, **kwargs: Any) -> requests.Response:
    return _request_with_handling("GET", url, **kwargs)


def http_post(url: str, **kwargs: Any) -> requests.Response:
    return _request_with_handling("POST", url, **kwargs)
