from __future__ import annotations

import logging
import re
import threading
import time
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any, cast

from app.common.http_client import DEFAULT_TIMEOUT, get_session
from app.common.settings import settings
from app.schemas.shopify_vendas_produtos import ShopifyPedido
from app.services.bling_planilha_shopify import (
    _linhas_por_pedido,
    enriquecer_bairros_nas_linhas,
    enriquecer_enderecos_nas_linhas,
)
from app.services.loader_main import carregar_skus
from app.utils.throttlers import (
    _GRAPHQL_BACKOFF_MAX,
    _GRAPHQL_BACKOFF_MIN,
    _GRAPHQL_BACKOFF_MULT,
    _sleep_throttle,
    _throttle_from_extensions,
)
from app.utils.utils_helpers import normalizar_order_id

from .shopify_client import _coletar_remaining_lineitems, _graphql_url, _http_shopify_headers

# -----------------------------------------------------------------------------
# logger
# -----------------------------------------------------------------------------
logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Shopify GraphQL basics
# -----------------------------------------------------------------------------


def query_vendas_shopify() -> str:
    """
    Query GraphQL para coletar pedidos da Shopify.
    Ajustes:
      - adicionamos $first configurável (padrão 50)
      - lineItems(first: 50) → pega título/sku/valores
      - fulfillmentOrders.lineItems(first: 50) → garante remainingQuantity
      - localizationExtensions(first: 10) → aumenta chance de capturar CPF
      - inclui campos de endereço relevantes
    """
    return """
    query($cursor: String, $search: String, $first: Int = 50) {
      orders(first: $first, after: $cursor, query: $search) {
        pageInfo { hasNextPage endCursor }
        edges {
          node {
            id
            name
            createdAt
            displayFulfillmentStatus
            currentTotalDiscountsSet { shopMoney { amount } }

            customer { email firstName lastName }

            shippingAddress {
              name
              address1
              address2
              city
              provinceCode
              zip
              phone
            }

            shippingLine {
              discountedPriceSet { shopMoney { amount } }
            }

            localizationExtensions(first: 10) {
              edges {
                node { purpose title value }
              }
            }

            lineItems(first: 50) {
              edges {
                node {
                  id
                  title
                  quantity
                  sku
                  product { id }
                  discountedTotalSet { shopMoney { amount } }
                }
              }
            }

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


def _paginacao_vendas_shopify(search_str: str) -> Iterable[ShopifyPedido]:
    """
    Iterador legado: percorre TODAS as páginas de pedidos, emitindo cada nó.
    Mantido para os fluxos de planilha já existentes.
    """
    url = _graphql_url()
    headers = _http_shopify_headers()
    cursor: str | None = None

    sess = get_session()
    backoff = _GRAPHQL_BACKOFF_MIN
    pagina = 0

    while True:
        body = {
            "query": query_vendas_shopify(),
            "variables": {"cursor": cursor, "search": search_str, "first": 50},
        }

        t0 = time.time()
        resp = sess.post(url, json=body, headers=headers, timeout=DEFAULT_TIMEOUT)
        dur = round(time.time() - t0, 3)

        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After")
            logger.warning(
                "graphql_http_429",
                extra={"retry_after_s": float(ra) if ra and ra.isdigit() else None, "duration_s": dur},
            )
            _sleep_throttle(float(ra) if ra and ra.isdigit() else backoff)
            backoff = min(backoff * _GRAPHQL_BACKOFF_MULT, _GRAPHQL_BACKOFF_MAX)
            continue

        resp.raise_for_status()
        payload = cast(dict[str, Any], resp.json() or {})

        errors = payload.get("errors") or []
        if errors:
            throttled = any("throttled" in str(e.get("message", "")).lower() for e in errors)
            wait, metrics = _throttle_from_extensions(payload)
            if throttled:
                logger.info(
                    "graphql_throttled",
                    extra={"duration_s": dur, "wait_s": round(wait, 3), **metrics, "pagina": pagina},
                )
                _sleep_throttle(wait if wait > 0 else backoff)
                backoff = min(backoff * _GRAPHQL_BACKOFF_MULT, _GRAPHQL_BACKOFF_MAX)
                continue
            logger.error("graphql_errors", extra={"errors": errors, "duration_s": dur, "pagina": pagina})
            raise RuntimeError(str(errors))

        # sucesso → reseta backoff
        backoff = _GRAPHQL_BACKOFF_MIN
        data = cast(dict[str, Any], payload.get("data") or {})
        orders = cast(dict[str, Any], data.get("orders") or {})
        edges = cast(list[dict[str, Any]], orders.get("edges") or [])
        page_info = cast(dict[str, Any], orders.get("pageInfo") or {})
        has_next = bool(page_info.get("hasNextPage"))
        end_cursor = page_info.get("endCursor")

        logger.info(
            "graphql_page_ok",
            extra={"duration_s": dur, "edges": len(edges), "has_next": has_next, "pagina": pagina},
        )

        for edge in edges:
            yield cast(ShopifyPedido, edge.get("node") or {})

        if not has_next:
            break
        pagina += 1
        cursor = cast(str | None, end_cursor)


def _pagina_vendas_shopify(
    *,
    search_str: str,
    cursor: str | None,
    first: int,
) -> tuple[list[ShopifyPedido], str | None]:
    """
    Busca UMA página de pedidos (até `first` itens) e retorna (lista_de_pedidos, next_cursor).
    Respeita 429 (Retry-After) e throttling via extensões GraphQL, semelhante ao iterador legado.
    """
    url = _graphql_url()
    headers = _http_shopify_headers()
    sess = get_session()

    # backoff simples em caso de 429/THROTTLED (mantemos consistente com o fluxo legado)
    backoff = _GRAPHQL_BACKOFF_MIN

    while True:
        body = {
            "query": query_vendas_shopify(),
            "variables": {"cursor": cursor, "search": search_str, "first": max(1, min(first, 50))},
        }

        t0 = time.time()
        resp = sess.post(url, json=body, headers=headers, timeout=DEFAULT_TIMEOUT)
        dur = round(time.time() - t0, 3)

        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After")
            logger.warning(
                "graphql_http_429_single",
                extra={"retry_after_s": float(ra) if ra and ra.isdigit() else None, "duration_s": dur},
            )
            _sleep_throttle(float(ra) if ra and ra.isdigit() else backoff)
            backoff = min(backoff * _GRAPHQL_BACKOFF_MULT, _GRAPHQL_BACKOFF_MAX)
            continue

        resp.raise_for_status()
        payload = cast(dict[str, Any], resp.json() or {})

        errors = payload.get("errors") or []
        if errors:
            throttled = any("throttled" in str(e.get("message", "")).lower() for e in errors)
            wait, metrics = _throttle_from_extensions(payload)
            if throttled:
                logger.info(
                    "graphql_throttled_single",
                    extra={"duration_s": dur, "wait_s": round(wait, 3), **metrics},
                )
                _sleep_throttle(wait if wait > 0 else backoff)
                backoff = min(backoff * _GRAPHQL_BACKOFF_MULT, _GRAPHQL_BACKOFF_MAX)
                continue
            logger.error("graphql_errors_single", extra={"errors": errors, "duration_s": dur})
            raise RuntimeError(str(errors))

        data = cast(dict[str, Any], payload.get("data") or {})
        orders = cast(dict[str, Any], data.get("orders") or {})
        edges = cast(list[dict[str, Any]], orders.get("edges") or [])
        page_info = cast(dict[str, Any], orders.get("pageInfo") or {})
        has_next = bool(page_info.get("hasNextPage"))
        end_cursor = cast(str | None, page_info.get("endCursor"))

        pedidos: list[ShopifyPedido] = []
        for e in edges:
            pedidos.append(cast(ShopifyPedido, (e or {}).get("node") or {}))

        logger.info(
            "graphql_single_page_ok",
            extra={"duration_s": dur, "edges": len(edges), "has_next": has_next},
        )

        return pedidos, (end_cursor if has_next else None)


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
        oid_norm = normalizar_order_id(oid)
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
    sku_produtos: list[str] | None = None,
) -> list[dict[str, Any]]:
    t0 = time.time()
    search = _parametros_coleta_shopify(data_inicio, fulfillment_status)
    skus_info = cast(Mapping[str, Mapping[str, Any]], carregar_skus())
    modo_fs = (fulfillment_status or "any").strip().lower()
    sku_filter = {s.strip().upper() for s in (sku_produtos or []) if s.strip()}

    logger.info(
        "coleta_inicio",
        extra={
            "data_inicio": data_inicio,
            "fulfillment_status": modo_fs,
            "sku_filter": sorted(sku_filter) or None,
        },
    )

    total_pedidos = 0
    total_linhas = 0
    linhas: list[dict[str, Any]] = []

    for pedido in _paginacao_vendas_shopify(search):
        total_pedidos += 1

        # ① remainingQuantity por lineItem (mesma página)
        remaining = _coletar_remaining_lineitems(pedido)

        # ② linhas do pedido
        linhas_pedido = _linhas_por_pedido(
            pedido=pedido,
            modo_fs=modo_fs,
            produto_alvo=None,
            skus_info=skus_info,
            remaining_por_line=remaining,
        )

        # ③ CPF direto do mesmo pedido (sem segunda chamada)
        try:
            cpf = ""
            edges = (pedido.get("localizationExtensions", {}) or {}).get("edges", []) or []
            for e in edges:
                node = (e or {}).get("node", {}) or {}
                title = str(node.get("title", "")).lower()
                purpose = str(node.get("purpose", "")).lower()
                if "cpf" in title or purpose == "tax":
                    import re as _re

                    cpf_raw = str(node.get("value", ""))
                    cpf = _re.sub(r"\D", "", cpf_raw)[:11]
                    if cpf:
                        break
            if cpf:
                for l in linhas_pedido:
                    if not str(l.get("CPF/CNPJ Comprador", "")).strip():
                        l["CPF/CNPJ Comprador"] = cpf
        except Exception:
            pass

        # ④ filtro por SKUs (se solicitado)
        if sku_filter:
            linhas_pedido = [l for l in linhas_pedido if str(l.get("SKU", "")).strip().upper() in sku_filter]

        linhas.extend(linhas_pedido)
        total_linhas += len(linhas_pedido)

        if total_pedidos % 10 == 0:
            logger.info(
                "coleta_parcial",
                extra={"pedidos_processados": total_pedidos, "linhas_acumuladas": total_linhas},
            )

    logger.info(
        "coleta_fim",
        extra={
            "pedidos_processados": total_pedidos,
            "linhas_total": total_linhas,
            "duration_s": round(time.time() - t0, 3),
        },
    )
    return linhas


def coletar_vendas_shopify(
    *,
    data_inicio: str,
    fulfillment_status: str = "any",
    sku_produtos: list[str] | None = None,
    enrich_cpfs: bool = True,
    enrich_bairros: bool = True,
    enrich_enderecos: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, int]]]:
    t0 = time.time()

    # 1) COLETAR (CPF já injetado na base quando disponível no payload)
    linhas = _coletar_pedidos_shopify_base(
        data_inicio=data_inicio,
        fulfillment_status=fulfillment_status,
        sku_produtos=sku_produtos,
    )

    # 2) NORMALIZAR ENDEREÇOS — determinístico; escreve "s/n" e "Precisa Contato"
    #    (com exceção Brasília/DF) e não promove complemento para bairro.
    if linhas and enrich_enderecos:
        te = time.time()
        pre_sem_num = sum(1 for l in linhas if not str(l.get("Número Entrega", "")).strip())
        enriquecer_enderecos_nas_linhas(linhas)  # <- usa shopify_ajuste_endereco
        pos_sem_num = sum(1 for l in linhas if not str(l.get("Número Entrega", "")).strip())
        logger.info(
            "enrich_enderecos_ok",
            extra={
                "pre_sem_num": pre_sem_num,
                "pos_sem_num": pos_sem_num,
                "duration_s": round(time.time() - te, 3),
            },
        )

    # 3) ENRIQUECER BAIRROS — via brazilcep por CEP (Entrega/Comprador), sem sobrescrever quem já tem.
    if linhas and enrich_bairros:
        tb = time.time()
        pre_vazios = sum(
            1
            for l in linhas
            if (not str(l.get("Bairro Comprador", "")).strip()) or (not str(l.get("Bairro Entrega", "")).strip())
        )
        enriquecer_bairros_nas_linhas(
            linhas,
            usar_cep_entrega=True,
            usar_cep_comprador=True,
            timeout=5,
        )  # <- usa shopify_busca_bairro
        pos_vazios = sum(
            1
            for l in linhas
            if (not str(l.get("Bairro Comprador", "")).strip()) or (not str(l.get("Bairro Entrega", "")).strip())
        )
        logger.info(
            "enrich_bairros_ok",
            extra={
                "pre_vazios": pre_vazios,
                "pos_vazios": pos_vazios,
                "duration_s": round(time.time() - tb, 3),
            },
        )

    # 4) ENRIQUECER CPF — já veio no passo de coleta; mantemos log/contabilidade
    if linhas and enrich_cpfs:
        pendentes_out = sum(1 for l in linhas if not str(l.get("CPF/CNPJ Comprador", "")).strip())
        logger.info("enrich_cpfs_ok", extra={"pendentes_out": pendentes_out})

    # Contagens finais
    cont_status: dict[str, int] = {}
    cont_prod: dict[str, int] = {}
    for l in linhas:
        st = (l.get("status_fulfillment") or "").upper()
        cont_status[st] = cont_status.get(st, 0) + 1
        prod = l.get("Produto") or ""
        if prod:
            cont_prod[prod] = cont_prod.get(prod, 0) + 1

    logger.info(
        "coleta_total_ok",
        extra={"linhas": len(linhas), "duration_s": round(time.time() - t0, 3)},
    )
    return linhas, {"status_fulfillment": cont_status, "produto": cont_prod}


# -----------------------------------------------------------------------------
# NOVO: Função pública para a rota HTTP
# -----------------------------------------------------------------------------


from collections.abc import Callable
from typing import Optional

from app.services.shopify_ajuste_endereco import normalizar_endereco_unico

# se você tiver um provider de IA, importe-o e passe aqui; senão deixe None
AiProvider = Optional[Callable[[str], Any]]


def _extrair_cpf_do_node(node: dict[str, Any]) -> str | None:
    """
    Lê CPF de localizationExtensions do próprio payload GraphQL.
    Regras:
      - purpose == "TAX" e/ou título contendo 'cpf'
      - retorna 11 dígitos (sanitizado)
    """
    try:
        edges = (node.get("localizationExtensions") or {}).get("edges") or []
        for e in edges:
            n = (e or {}).get("node") or {}
            title = str(n.get("title") or "").lower()
            purpose = str(n.get("purpose") or "").lower()
            if "cpf" in title or purpose == "tax":
                import re as _re

                cpf = _re.sub(r"\D", "", str(n.get("value") or ""))[:11]
                if len(cpf) == 11:
                    return cpf
    except Exception:
        pass
    return None


def listar_pedidos_shopify(
    *,
    data_inicio: str,  # dd/MM/yyyy (padrão do repo)
    status: str = "any",  # "any" | "unfulfilled"
    usar_gpt: bool = False,  # ativa fallback por IA na normalização
) -> tuple[list[ShopifyPedido], None]:
    """
    Retorna TODOS os pedidos a partir de data_inicio.
    Sempre faz:
      - ACHATA lineItems.edges -> list[node]
      - extrai CPF
      - normaliza endereço (inclui bairro/logradouro oficiais via CEP)
    Opcional:
      - ajuste com IA (se usar_gpt=True e houver provider configurado no normalizador)
    """
    # 1) Filtros da busca
    search = _parametros_coleta_shopify(data_inicio, status)

    pedidos_norm: list[ShopifyPedido] = []

    # 2) Itera TODAS as páginas da Shopify
    for node in _paginacao_vendas_shopify(search):
        # --- 2.1 Achatar line items ---
        li_edges = (node.get("lineItems") or {}).get("edges") or []
        line_items: list[dict[str, Any]] = []
        for lie in li_edges:
            inode = (lie or {}).get("node") or {}
            line_items.append(
                {
                    "id": inode.get("id"),
                    "title": inode.get("title"),
                    "quantity": inode.get("quantity"),
                    "sku": inode.get("sku"),
                    "product": (inode.get("product") or {}),
                    "discountedTotalSet": (inode.get("discountedTotalSet") or {}),
                }
            )

        # --- 2.2 Extrair CPF ---
        cpf = _extrair_cpf_do_node(node)

        # --- 2.3 Normalizar endereço SEMPRE (usa CEP internamente) ---
        addr_in = node.get("shippingAddress") or {}
        order_id = str(node.get("id") or node.get("name") or "")
        address1 = str(addr_in.get("address1") or "")
        address2 = str(addr_in.get("address2") or "")
        cep = str(addr_in.get("zip") or "") or None

        # provider de IA (se você tiver um, injete no normalizador; caso contrário fica None)
        ai_provider = None
        if usar_gpt:
            ai_provider = None  # substitua aqui pelo seu provider quando disponível

        try:
            norm = normalizar_endereco_unico(
                order_id=order_id,
                address1=address1,
                address2=address2,
                cep=cep,
                ai_provider=ai_provider,
            )
        except Exception:
            # fallback: mantém originais
            norm = {
                "endereco_base": address1,
                "numero": None,
                "complemento": address2,
                "logradouro_oficial": None,
                "bairro_oficial": None,
                "precisa_contato": None,
            }

        shipping_address_out: dict[str, Any] = dict(addr_in or {})
        # aplica mínimos obrigatórios
        shipping_address_out["address1"] = norm.get("endereco_base") or address1
        shipping_address_out["address2"] = norm.get("complemento") or address2
        if norm.get("numero") is not None:
            shipping_address_out["numero"] = norm.get("numero")
        if norm.get("bairro_oficial"):
            shipping_address_out["bairro_oficial"] = norm["bairro_oficial"]
        if norm.get("logradouro_oficial"):
            shipping_address_out["logradouro_oficial"] = norm["logradouro_oficial"]
        if "precisa_contato" in norm:
            shipping_address_out["precisa_contato"] = norm.get("precisa_contato")

        pedido: dict[str, Any] = {
            "id": node.get("id"),
            "name": node.get("name"),
            "createdAt": node.get("createdAt"),
            "displayFulfillmentStatus": node.get("displayFulfillmentStatus"),
            "currentTotalDiscountsSet": (node.get("currentTotalDiscountsSet") or {}),
            "customer": (node.get("customer") or {}),
            "shippingAddress": shipping_address_out,
            "shippingLine": (node.get("shippingLine") or {}),
            "lineItems": line_items,
            "cpf": cpf,
        }

        pedidos_norm.append(cast(ShopifyPedido, pedido))

    logger.info(
        "listar_pedidos_shopify_ok",
        extra={
            "items": len(pedidos_norm),
            "status": status,
            "usar_gpt": usar_gpt,
            "paginacao": "all_pages",
        },
    )
    return pedidos_norm, None
