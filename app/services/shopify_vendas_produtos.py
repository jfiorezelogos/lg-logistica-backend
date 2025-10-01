from __future__ import annotations

import logging
import re
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime
from typing import Any, cast

import requests
from utils.utils_helpers import _normalizar_order_id

from app.common.settings import settings
from app.schemas.shopify_vendas_produtos import _Pedido
from app.services.loader_main import carregar_skus
from app.services.shopify_normalizar_gpt import normalizar_enderecos_batch
from app.services.shopify_planilha import _linhas_por_pedido, enriquecer_bairros_nas_linhas

from .shopify_client import _graphql_url, _http_shopify_headers

# -----------------------------------------------------------------------------
# logger
# -----------------------------------------------------------------------------
logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Shopify GraphQL basics
# -----------------------------------------------------------------------------


def _mk_query_orders() -> str:
    # inclui campos usados na transformação (endereços, valores, line items e fulfillmentOrders p/ remaining)
    return """
    query($cursor: String, $search: String) {
      orders(first: 50, after: $cursor, query: $search) {
        pageInfo { hasNextPage endCursor }
        edges {
          node {
            id
            name
            createdAt
            displayFulfillmentStatus
            customer { firstName lastName email }
            shippingAddress {
              address1 address2 city provinceCode zip phone
            }
            currentTotalDiscountsSet { shopMoney { amount } }
            shippingLine { discountedPriceSet { shopMoney { amount } } }
            lineItems(first: 50) {
              edges {
                node {
                  id
                  quantity
                  discountedTotalSet { shopMoney { amount } }
                  product { id }
                }
              }
            }
            fulfillmentOrders(first: 10) {
              edges {
                node {
                  status
                  lineItems(first: 50) {
                    edges {
                      node {
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
      }
    }
    """.strip()


def _build_search(data_inicio_ddmmyyyy: str, fulfillment_status: str) -> str:
    # created_at:>=YYYY-MM-DD + fulfillment_status opcional
    try:
        dt = datetime.strptime(data_inicio_ddmmyyyy, "%d/%m/%Y")
    except Exception:
        raise ValueError('data_inicio inválida: use "dd/MM/yyyy"')

    filtros: list[str] = [f'created_at:>={dt.strftime("%Y-%m-%d")}']
    if (fulfillment_status or "any").strip().lower() == "unfulfilled":
        filtros.append("fulfillment_status:unfulfilled")
    return " ".join(filtros)


def _paginado_orders(search_str: str) -> Iterable[_Pedido]:
    url = _graphql_url()
    headers = _http_shopify_headers()
    cursor: str | None = None

    while True:
        body = {"query": _mk_query_orders(), "variables": {"cursor": cursor, "search": search_str}}
        resp = requests.post(url, json=body, headers=headers, timeout=12, verify=False)
        if resp.status_code == 429:
            time.sleep(int(resp.headers.get("Retry-After", "2")))
            continue
        resp.raise_for_status()
        payload = cast(dict[str, Any], resp.json())

        if payload.get("errors"):
            raise RuntimeError(str(payload["errors"]))

        data = cast(dict[str, Any], payload.get("data") or {})
        orders = cast(dict[str, Any], data.get("orders") or {})
        for edge in cast(list[dict[str, Any]], orders.get("edges") or []):
            node = cast(_Pedido, edge.get("node") or {})
            yield node

        page = cast(dict[str, Any], orders.get("pageInfo") or {})
        if not page.get("hasNextPage"):
            break
        cursor = cast(str | None, page.get("endCursor"))


def _coletar_remaining_por_line(pedido: Mapping[str, Any]) -> dict[str, int]:
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


def coletar_vendas_produtos(
    data_inicio: str,
    fulfillment_status: str = "any",
    produto_alvo: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, int]]]:
    search = _build_search(data_inicio, fulfillment_status)
    skus_info = cast(Mapping[str, Mapping[str, Any]], carregar_skus())

    modo_fs = (fulfillment_status or "any").strip().lower()
    linhas: list[dict[str, Any]] = []

    for pedido in _paginado_orders(search):
        linhas.extend(_linhas_por_pedido(pedido, modo_fs, produto_alvo, skus_info))

    cont_status: dict[str, int] = {}
    cont_prod: dict[str, int] = {}
    for l in linhas:
        st = (l.get("status_fulfillment") or "").upper()
        cont_status[st] = cont_status.get(st, 0) + 1
        prod = l.get("Produto") or ""
        if prod:
            cont_prod[prod] = cont_prod.get(prod, 0) + 1

    contagem = {"status_fulfillment": cont_status, "produto": cont_prod}
    return linhas, contagem


# -----------------------------------------------------------------------------
# CPF via GraphQL (reaproveite seu provider existente, se já houver)
# -----------------------------------------------------------------------------
_MIN_INTERVALO_GQL = 0.6
_gql_lock = threading.Lock()
_gql_ultimo = 0.0


def obter_cpfs_bulk(order_ids: Iterable[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    sess = requests.Session()
    sess.headers.update({"Content-Type": "application/json", "X-Shopify-Access-Token": settings.SHOPIFY_TOKEN})

    for oid in order_ids:
        oid_norm = _normalizar_order_id(oid)
        gid = f"gid://shopify/Order/{oid_norm}"

        # throttle
        global _gql_ultimo
        with _gql_lock:
            delta = time.time() - _gql_ultimo
            if delta < _MIN_INTERVALO_GQL:
                time.sleep(_MIN_INTERVALO_GQL - delta)
            _gql_ultimo = time.time()

        q = {
            "query": f"""
            {{
              order(id: "{gid}") {{
                localizationExtensions(first: 10) {{
                  edges {{
                    node {{ purpose title value }}
                  }}
                }}
              }}
            }}
            """
        }
        try:
            r = sess.post(_graphql_url(), json=q, timeout=10, verify=False)
            if r.status_code == 200:
                data = r.json()
                edges = data.get("data", {}).get("order", {}).get("localizationExtensions", {}).get("edges", [])
                cpf = ""
                for e in edges:
                    node = (e or {}).get("node", {})
                    if node.get("purpose") == "TAX" and "cpf" in str(node.get("title", "")).lower():
                        cpf = re.sub(r"\D", "", str(node.get("value", "")))[:11]
                        break
                if cpf:
                    out[oid_norm] = cpf
        except Exception:
            pass

    return out


def enriquecer_cpfs_nas_linhas(linhas: list[dict[str, str]], mapa_cpfs: dict[str, str]) -> None:
    for l in linhas:
        if not l.get("CPF/CNPJ Comprador"):
            tid = _normalizar_order_id(l.get("transaction_id", ""))
            if tid and tid in mapa_cpfs:
                l["CPF/CNPJ Comprador"] = mapa_cpfs[tid]


def coletar_vendas_shopify(
    *,
    data_inicio: str,
    fulfillment_status: str = "any",
    produto_alvo: str | None = None,
    ids_shopify: list[str] | None = None,
    enrich_cpfs: bool = True,
    enrich_bairros: bool = True,
    enrich_enderecos: bool = True,
    use_ai_enderecos: bool = True,
    ai_provider: Callable[[str], Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, int]]]:
    linhas, _ = coletar_vendas_produtos(
        data_inicio=data_inicio,
        fulfillment_status=fulfillment_status,
        produto_alvo=produto_alvo,
    )

    if ids_shopify:
        ids_set = {str(i).strip() for i in ids_shopify}
        linhas = [l for l in linhas if str(l.get("id_produto", "")).strip() in ids_set]

    if enrich_bairros and linhas:
        enriquecer_bairros_nas_linhas(linhas)

    if enrich_enderecos and linhas:
        normalizar_enderecos_batch(linhas, ai_provider=ai_provider if use_ai_enderecos else None)

    if enrich_cpfs and linhas:
        pendentes = {
            str(l.get("transaction_id", "")).strip() for l in linhas if not str(l.get("CPF/CNPJ Comprador", "")).strip()
        }
        pendentes = {p for p in pendentes if p}
        if pendentes:
            mapa = obter_cpfs_bulk(pendentes)
            if mapa:
                enriquecer_cpfs_nas_linhas(linhas, mapa)

    # contagens finais
    cont_status: dict[str, int] = {}
    cont_prod: dict[str, int] = {}
    for l in linhas:
        st = (l.get("status_fulfillment") or "").upper()
        cont_status[st] = cont_status.get(st, 0) + 1
        prod = l.get("Produto") or ""
        if prod:
            cont_prod[prod] = cont_prod.get(prod, 0) + 1

    return linhas, {"status_fulfillment": cont_status, "produto": cont_prod}
