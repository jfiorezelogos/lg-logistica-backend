from __future__ import annotations

import logging
import re
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime
from typing import Any, cast

from app.common.http_client import DEFAULT_TIMEOUT, get_session
from app.common.settings import settings
from app.schemas.shopify_vendas_produtos import _Pedido
from app.services.bling_planilha_shopify import (
    _linhas_por_pedido,
    enriquecer_bairros_nas_linhas,
    enriquecer_cpfs_nas_linhas,
)
from app.services.loader_main import carregar_skus
from app.services.shopify_normalizar_gpt import normalizar_enderecos_batch
from app.utils.utils_helpers import _normalizar_order_id

from .shopify_client import _coletar_remaining_lineitems, _graphql_url, _http_shopify_headers

# -----------------------------------------------------------------------------
# logger
# -----------------------------------------------------------------------------
logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Shopify GraphQL basics
# -----------------------------------------------------------------------------


def query_vendas_shopify() -> str:
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


def _parametros_coleta_shopify(data_inicio_ddmmyyyy: str, fulfillment_status: str) -> str:
    # created_at:>=YYYY-MM-DD + fulfillment_status opcional
    try:
        dt = datetime.strptime(data_inicio_ddmmyyyy, "%d/%m/%Y")
    except Exception:
        raise ValueError('data_inicio inválida: use "dd/MM/yyyy"')

    filtros: list[str] = [
        f'created_at:>={dt.strftime("%Y-%m-%d")}',
        "financial_status:paid",
    ]
    if (fulfillment_status or "any").strip().lower() == "unfulfilled":
        filtros.append("fulfillment_status:unfulfilled")
    return " ".join(filtros)


def _paginacao_vendas_shopify(search_str: str) -> Iterable[_Pedido]:
    url = _graphql_url()
    headers = _http_shopify_headers()
    cursor: str | None = None

    sess = get_session()  # sessão global com retry/backoff
    while True:
        body = {"query": query_vendas_shopify(), "variables": {"cursor": cursor, "search": search_str}}
        resp = sess.post(url, json=body, headers=headers, timeout=DEFAULT_TIMEOUT)
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


# -----------------------------------------------------------------------------
# CPF via GraphQL (reaproveite seu provider existente, se já houver)
# -----------------------------------------------------------------------------
_MIN_INTERVALO_GQL = 0.6
_gql_lock = threading.Lock()
_gql_ultimo = 0.0


def obter_cpfs_pedidos_shopify(order_ids: Iterable[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    sess = get_session()
    headers = {"Content-Type": "application/json", "X-Shopify-Access-Token": settings.SHOPIFY_TOKEN}

    for oid in order_ids:
        oid_norm = _normalizar_order_id(oid)
        gid = f"gid://shopify/Order/{oid_norm}"

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
            r = sess.post(_graphql_url(), json=q, headers=headers, timeout=DEFAULT_TIMEOUT)
            if r.status_code == 200:
                data = r.json()
                edges = data.get("data", {}).get("order", {}).get("localizationExtensions", {}).get("edges", []) or []
                cpf = ""
                for e in edges:
                    node = (e or {}).get("node", {})
                    if node.get("purpose") == "TAX" and "cpf" in str(node.get("title", "")).lower():
                        cpf = re.sub(r"\D", "", str(node.get("value", "")))[:11]
                        break
                if cpf:
                    out[oid_norm] = cpf
        except Exception:
            # resiliente: segue com os próximos
            pass

    return out


def _coletar_pedidos_shopify_base(
    *,
    data_inicio: str,
    fulfillment_status: str = "any",
    sku_produtos: list[str] | None = None,  # filtro opcional por SKUs internos
) -> list[dict[str, Any]]:
    search = _parametros_coleta_shopify(data_inicio, fulfillment_status)
    skus_info = cast(Mapping[str, Mapping[str, Any]], carregar_skus())

    modo_fs = (fulfillment_status or "any").strip().lower()
    sku_filter = {s.strip().upper() for s in (sku_produtos or []) if s.strip()}
    linhas: list[dict[str, Any]] = []

    for pedido in _paginacao_vendas_shopify(search):
        # use o nome consistente que você tiver exportado (ex.: do shopify_common)
        remaining = _coletar_remaining_lineitems(pedido)  # ou coletar_remaining_por_line(pedido)
        linhas_pedido = _linhas_por_pedido(
            pedido=pedido,
            modo_fs=modo_fs,
            produto_alvo=None,  # não filtramos por nome
            skus_info=skus_info,
            remaining_por_line=remaining,
        )
        # filtro por SKU, se houver
        if sku_filter:
            linhas.extend(l for l in linhas_pedido if str(l.get("SKU", "")).strip().upper() in sku_filter)
        else:
            linhas.extend(linhas_pedido)

    return linhas


def coletar_vendas_shopify(
    *,
    data_inicio: str,
    fulfillment_status: str = "any",
    sku_produtos: list[str] | None = None,  # filtro por SKUs
    enrich_cpfs: bool = True,  # sempre True
    enrich_bairros: bool = True,  # sempre True
    enrich_enderecos: bool = True,  # sempre True
    use_ai_enderecos: bool = True,
    ai_provider: Callable[[str], Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, int]]]:

    linhas = _coletar_pedidos_shopify_base(
        data_inicio=data_inicio,
        fulfillment_status=fulfillment_status,
        sku_produtos=sku_produtos,
    )

    # enriquecimentos sempre ligados
    if linhas and enrich_bairros:
        enriquecer_bairros_nas_linhas(linhas)

    if linhas and enrich_enderecos:
        normalizar_enderecos_batch(linhas, ai_provider=ai_provider if use_ai_enderecos else None)

    if linhas and enrich_cpfs:
        pendentes = {
            str(l.get("transaction_id", "")).strip() for l in linhas if not str(l.get("CPF/CNPJ Comprador", "")).strip()
        }
        pendentes = {p for p in pendentes if p}
        if pendentes:
            # use o nome correto da sua função de CPF (ajuste se for obter_cpfs_bulk)
            mapa = obter_cpfs_pedidos_shopify(pendentes)
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
