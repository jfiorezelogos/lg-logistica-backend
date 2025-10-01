from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from app.common.settings import settings


def obter_api_shopify_version(now: datetime | None = None) -> str:
    """
    Retorna a versão trimestral da Shopify API (YYYY-01/04/07/10).
    Usa datetime aware (UTC por padrão). 'now' é opcional (útil para testes).
    """
    dt = now or datetime.now(UTC)
    y, m = dt.year, dt.month
    q_start = ((m - 1) // 3) * 3 + 1  # 1, 4, 7, 10
    return f"{y}-{q_start:02d}"


API_VERSION = obter_api_shopify_version()
GRAPHQL_URL = f"https://{settings.SHOP_URL}/admin/api/{API_VERSION}/graphql.json"
REST_URL = f"https://{settings.SHOP_URL}/admin/api/{API_VERSION}"  # útil para endpoints REST


def _graphql_url() -> str:
    return f"https://{settings.SHOP_URL}/admin/api/{obter_api_shopify_version()}/graphql.json"


def _http_shopify_headers() -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": settings.SHOPIFY_TOKEN,
    }


def _coletar_remaining_lineitems(pedido: Mapping[str, Any]) -> dict[str, int]:
    remaining: dict[str, int] = {}
    fo_edges = (pedido.get("fulfillmentOrders") or {}).get("edges") or []
    for fo_e in fo_edges:
        fo_node = (fo_e or {}).get("node") or {}
        li_edges = ((fo_node.get("lineItems") or {}).get("edges")) or []
        for li_e in li_edges:
            li_node = (li_e or {}).get("node") or {}
            gid = ((li_node.get("lineItem") or {}) or {}).get("id") or ""
            lid = str(gid).split("/")[-1] if gid else ""
            rq = int(li_node.get("remainingQuantity") or 0)
            if lid:
                remaining[lid] = max(remaining.get(lid, 0), rq)
    return remaining
