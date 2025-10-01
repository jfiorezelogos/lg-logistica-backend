import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

# limites e backoff
_GRAPHQL_BACKOFF_MIN = 0.5
_GRAPHQL_BACKOFF_MAX = 10.0
_GRAPHQL_BACKOFF_MULT = 1.6


def _sleep_throttle(seconds: float) -> None:
    s = max(0.0, min(seconds, _GRAPHQL_BACKOFF_MAX))
    if s > 0:
        logger.info("graphql_throttle_sleep", extra={"sleep_s": round(s, 3)})
        time.sleep(s)


def _throttle_from_extensions(payload: dict[str, Any]) -> tuple[float, dict[str, float]]:
    """
    Retorna (wait_seconds, metrics) onde metrics = {requested, available, restore_rate}.
    """
    try:
        ext = (payload.get("extensions") or {}).get("cost") or {}
        ts = ext.get("throttleStatus") or {}
        requested = float(ext.get("requestedQueryCost", ext.get("actualQueryCost", 0)) or 0)
        available = float(ts.get("currentlyAvailable", 0) or 0)
        restore = float(ts.get("restoreRate", 0) or 0)
        wait = (requested - available) / restore if restore > 0 and requested > available else 0.0
        return max(0.0, wait), {"requested": requested, "available": available, "restore_rate": restore}
    except Exception:
        return 0.0, {"requested": 0.0, "available": 0.0, "restore_rate": 0.0}
