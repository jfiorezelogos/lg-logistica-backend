from __future__ import annotations

from collections.abc import Iterable
from typing import Any, cast

import requests

from app.common.http_client import DEFAULT_TIMEOUT, get_session
from app.common.settings import settings
from app.schemas.shopify_fulfillment import (
    FulfillBatchRequest,
    FulfillBatchResponse,
    FulfillOrderResult,
)
from app.utils.throttlers import (
    _GRAPHQL_BACKOFF_MAX,
    _GRAPHQL_BACKOFF_MIN,
    _GRAPHQL_BACKOFF_MULT,
    _sleep_throttle,
)
from app.utils.utils_helpers import normalizar_order_id  # você disse que já existe

# Se já existirem helpers padronizados, use-os:
try:
    from app.services.shopify_client import _graphql_url, _http_shopify_headers  # preferencial
except Exception:

    def _graphql_url() -> str:
        base = f"https://{settings.SHOP_URL}".rstrip("/")
        api = settings.SHOPIFY_TOKEN.strip("/")
        return f"{base}/admin/api/{api}/graphql.json"

    def _http_shopify_headers() -> dict[str, str]:
        return {"Content-Type": "application/json", "X-Shopify-Access-Token": settings.SHOPIFY_TOKEN}


_QUERY_FO = """
query($orderId: ID!) {
  order(id: $orderId) {
    fulfillmentOrders(first: 10) {
      edges {
        node {
          id
          status
          lineItems(first: 50) {
            edges {
              node {
                id
                remainingQuantity
                lineItem { id }
              }
            }
          }
        }
      }
    }
  }
}
""".strip()

_MUTATION_CREATE = """
mutation fulfillmentCreate($fulfillment: FulfillmentV2Input!) {
  fulfillmentCreateV2(fulfillment: $fulfillment) {
    fulfillment { id status }
    userErrors { field message }
  }
}
""".strip()


def _post_graphql(body: dict[str, Any]) -> requests.Response:
    sess = get_session()
    url = _graphql_url()
    headers = _http_shopify_headers()
    return sess.post(url, json=body, headers=headers, timeout=DEFAULT_TIMEOUT)


def _montar_payloads_fulfillment(order_gid: str, solicitados: Iterable[str]) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Retorna (payloads, ignored), onde:
      - payloads: lista de {"fulfillmentOrderId": ..., "fulfillmentOrderLineItems": [{"id": FOItemId, "quantity": remaining}, ...]}
      - ignored: ids (numéricos) solicitados mas que não estavam pendentes no FO correspondente
    """
    # 1) Buscar fulfillmentOrders do pedido
    backoff = _GRAPHQL_BACKOFF_MIN
    while True:
        r = _post_graphql({"query": _QUERY_FO, "variables": {"orderId": order_gid}})
        if r.status_code == 429:
            # respeita rate limit HTTP
            ra = r.headers.get("Retry-After")
            _sleep_throttle(float(ra) if ra and ra.isdigit() else backoff)
            backoff = min(backoff * _GRAPHQL_BACKOFF_MULT, _GRAPHQL_BACKOFF_MAX)
            continue
        r.raise_for_status()
        data = cast(dict[str, Any], r.json() or {})
        break

    edges = (((data.get("data") or {}).get("order") or {}).get("fulfillmentOrders", {}) or {}).get("edges", []) or []

    solicitados_norm = {normalizar_order_id(i) for i in solicitados if i}
    ignored: list[str] = []
    payloads: list[dict[str, Any]] = []

    # 2) Montar lineItemsByFulfillmentOrder
    for e in edges:
        fo = (e or {}).get("node") or {}
        if (fo.get("status") or "").upper() != "OPEN":
            continue

        li_edges = (fo.get("lineItems") or {}).get("edges") or []
        items_for_this_fo: list[dict[str, Any]] = []

        for lie in li_edges:
            li = (lie or {}).get("node") or {}
            line_item_gid = str(((li.get("lineItem") or {}).get("id")) or "")
            line_item_id = normalizar_order_id(line_item_gid)
            remaining = int(li.get("remainingQuantity") or 0)
            if remaining > 0 and line_item_id in solicitados_norm:
                items_for_this_fo.append(
                    {
                        "id": str(li.get("id") or ""),
                        "quantity": remaining,
                    }
                )

        if items_for_this_fo:
            payloads.append(
                {
                    "fulfillmentOrderId": str(fo.get("id") or ""),
                    "fulfillmentOrderLineItems": items_for_this_fo,
                }
            )

    # 3) Itens solicitados que não entraram em nenhum FO (sem remaining)
    if solicitados_norm:
        usados = {
            normalizar_order_id((it.get("lineItem") or {}).get("id") or "")  # type: ignore[index]
            for e in edges
            for itedge in (((e or {}).get("node") or {}).get("lineItems") or {}).get("edges", [])  # type: ignore[union-attr]
            for it in [(itedge or {}).get("node") or {}]
        }
        # Marcar como ignorados apenas os explicitamente solicitados que não têm remaining (>0)
        for s in solicitados_norm:
            if s not in usados:
                ignored.append(s)

    return payloads, ignored


def _executar_fulfillment(payloads: list[dict[str, Any]], notify_customer: bool) -> tuple[int, list[str]]:
    """
    Chama fulfillmentCreateV2. Retorna (qtd_total_enviada, user_errors_formatados).
    """
    if not payloads:
        return 0, []

    backoff = _GRAPHQL_BACKOFF_MIN
    body = {
        "query": _MUTATION_CREATE,
        "variables": {
            "fulfillment": {
                "notifyCustomer": bool(notify_customer),
                "lineItemsByFulfillmentOrder": payloads,
            }
        },
    }

    while True:
        r = _post_graphql(body)
        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            _sleep_throttle(float(ra) if ra and ra.isdigit() else backoff)
            backoff = min(backoff * _GRAPHQL_BACKOFF_MULT, _GRAPHQL_BACKOFF_MAX)
            continue
        r.raise_for_status()
        resp = cast(dict[str, Any], r.json() or {})
        break

    user_errors = ((resp.get("data") or {}).get("fulfillmentCreateV2") or {}).get("userErrors") or []  # type: ignore[union-attr]
    if user_errors:
        errs = []
        for e in user_errors:
            field = "/".join(map(str, (e.get("field") or [])))
            errs.append(f"{field} → {e.get('message')}")
        return 0, errs

    # soma total de quantities enviadas
    total = 0
    for fo in payloads:
        for it in fo.get("fulfillmentOrderLineItems", []):
            total += int(it.get("quantity") or 0)
    return total, []


def processar_fulfillments(req: FulfillBatchRequest) -> FulfillBatchResponse:
    """
    Processa vários pedidos/line items para fulfillment.
    Agrupa por pedido, calcula payloads por FO e executa a mutation.
    """
    results: list[FulfillOrderResult] = []
    total_fulfilled = 0

    for pedido in req.pedidos:
        oid_norm = normalizar_order_id(pedido.transaction_id)
        order_gid = f"gid://shopify/Order/{oid_norm}"

        payloads, ignored = _montar_payloads_fulfillment(order_gid, pedido.line_item_ids)
        if not payloads:
            results.append(
                FulfillOrderResult(
                    order_id=oid_norm, fulfilled_count=0, ignored_line_items=ignored, message="Nada pendente"
                )
            )
            continue

        qtd, errs = _executar_fulfillment(payloads, notify_customer=req.notify_customer)
        if errs:
            results.append(
                FulfillOrderResult(
                    order_id=oid_norm, fulfilled_count=0, ignored_line_items=ignored, message="; ".join(errs)
                )
            )
            continue

        total_fulfilled += qtd
        results.append(
            FulfillOrderResult(order_id=oid_norm, fulfilled_count=qtd, ignored_line_items=ignored, message=None)
        )

    return FulfillBatchResponse(ok=True, total_fulfilled=total_fulfilled, results=results)
