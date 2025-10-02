from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

from app.common.http_client import get_session
from app.common.settings import settings
from app.schemas.fretebarato_cotacao import (
    CotacaoOp,
    CotarFretesAutoRequest,
    CotarFretesResponse,
    ResultadoLote,
    TransportadoraEnum,
)

# --------------------------------------------------------------------
# Snapshot dos pedidos já coletados (linhas "planilha")
# Sua rota GET /shopify/pedidos deve alimentar este cache.
# --------------------------------------------------------------------
try:
    # Ideal: um módulo compartilhado atualizado pela rota de coleta
    from app.services.coletas_cache import get_planilha_atual  # type: ignore[attr-defined]
except Exception:
    _PLANILHA_CACHE: list[dict[str, Any]] = []
    _META: dict[str, Any] = {}

    def get_planilha_atual() -> tuple[list[dict[str, Any]], dict[str, Any]]:
        return _PLANILHA_CACHE, _META


# Loader de SKUs (peso/preço fallback) — se existir
try:
    from app.services.loader_produtos_info import load_skus_info  # type: ignore[attr-defined]
except Exception:
    load_skus_info = None


# ---------------------------
# Helpers
# ---------------------------
def _digits(s: str | None) -> str:
    return re.sub(r"\D", "", s or "")


def _norm_email(s: str | None) -> str:
    return (s or "").strip().lower()


def _norm_numero(s: str | None) -> str:
    """Extrai número com opcional 1 letra (ex.: 1500A)."""
    s = (s or "").strip()
    m = re.search(r"\b(\d{1,6}[A-Za-z]?)\b", s)
    return (m.group(1) if m else "").upper()


def _resolver_peso_preco_sku(
    sku: str,
    qty: int,
    skus_info: Mapping[str, Mapping[str, Any]] | None,
) -> tuple[float, float]:
    """Retorna (peso_unit, preco_fallback) para o SKU, ou (0.0, 0.0)."""
    if not skus_info:
        return 0.0, 0.0
    sku_up = sku.strip().upper()
    for info in skus_info.values():
        if str(info.get("sku", "")).strip().upper() == sku_up:
            peso_unit = float(info.get("peso", 0.0) or 0.0)
            preco_fallback = float(info.get("preco_fallback", 0.0) or 0.0)
            return peso_unit, preco_fallback
    return 0.0, 0.0


def _valor_total_linha(row: Mapping[str, Any]) -> float:
    """Lê 'Valor Total' já formatado (pt-BR), seguro a None/''."""
    try:
        v = str(row.get("Valor Total", "") or "0").replace(",", ".")
        return float(v) if v else 0.0
    except Exception:
        return 0.0


def _qty_from_row(row: Mapping[str, Any]) -> int:
    # Se tiver uma coluna de quantidade, use; senão infere 1 (seu desktop gera 1 por expansão)
    for key in ("Quantidade", "Qtd", "quantity"):
        if key in row:
            try:
                return int(str(row.get(key) or "1"))
            except Exception:
                return 1
    return 1


def _payload_fretebarato(cep8: str, total: float, peso: float) -> dict[str, Any]:
    return {
        "zipcode": cep8,
        "amount": round(float(total), 2),
        "skus": [
            {
                "sku": "B050A",  # simbólico
                "price": round(float(total), 2),
                "quantity": 1,
                "length": 24,
                "width": 16,
                "height": 3,
                "weight": round(float(peso), 3),
            }
        ],
    }


def _filtrar_por_transportadoras(
    quotes: Sequence[Mapping[str, Any]],
    selecionadas_up: set[str],
) -> list[CotacaoOp]:
    out: list[CotacaoOp] = []
    for q in quotes:
        try:
            nome = str(q.get("name", "")).strip().upper()
            if nome in selecionadas_up:
                out.append(
                    CotacaoOp(
                        nome_transportadora=TransportadoraEnum(nome),
                        nome_servico=(str(q.get("service", "")) or None),
                        valor=float(q.get("price", 0) or 0),
                    )
                )
        except Exception:
            continue
    return sorted(out, key=lambda x: x.valor)


# ---------------------------
# Principal: usa apenas LINHAS COLETADAS
# ---------------------------
def cotar_fretes_auto(req: CotarFretesAutoRequest) -> CotarFretesResponse:
    # 0) carrega snapshot das linhas
    linhas, _meta = get_planilha_atual()
    if not linhas:
        # Sem snapshot disponível → 409/412 seria ok no router; aqui só retornamos vazio
        return CotarFretesResponse(ok=True, resultados=[], total_lotes=0, total_com_frete=0)

    # 1) normaliza entradas e monta um conjunto de (email, cep8, numero)
    entradas_norm = {
        (_norm_email(e.email), _digits(e.cep).zfill(8), _norm_numero(e.numero_entrega)) for e in req.entradas
    }

    # 2) filtra linhas que batem com QUALQUER das entradas
    # colunas esperadas na planilha (desktop/serviço):
    #   "E-mail Comprador", "CEP Entrega", "Número Entrega", "SKU", "Valor Total", ...
    selecionadas_set = {s.value for s in req.selecionadas}
    linhas_match: list[dict[str, Any]] = []
    for row in linhas:
        email = _norm_email(row.get("E-mail Comprador") or row.get("Email") or "")
        cep8 = _digits(row.get("CEP Entrega") or row.get("CEP") or "").zfill(8)
        numero = _norm_numero(row.get("Número Entrega") or row.get("Numero") or row.get("address2") or "")
        if (email, cep8, numero) in entradas_norm:
            linhas_match.append(row)

    # 3) agrupa por (email, cep8) → id_lote L0001, L0002...
    grupos: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in linhas_match:
        email = _norm_email(row.get("E-mail Comprador") or row.get("Email") or "")
        cep8 = _digits(row.get("CEP Entrega") or row.get("CEP") or "").zfill(8)
        grupos[(email, cep8)].append(row)

    # 4) carrega catálogo de SKUs (peso/preço fallback) se existir
    skus_info = None
    if callable(load_skus_info):
        try:
            skus_info = load_skus_info()
        except Exception:
            skus_info = None

    # 5) para cada lote, calcula valor_total/peso_total e cota
    sess = get_session()
    resultados: list[ResultadoLote] = []
    seq = 1

    for (email, cep8), rows in grupos.items():
        id_lote = f"L{seq:04d}"
        seq += 1

        if not rows:
            resultados.append(
                ResultadoLote(
                    id_lote=id_lote,
                    email=email,
                    cep=cep8,
                    melhor=None,
                    todas=[],
                    mensagem="Nenhuma linha no lote",
                    valor_total=0.0,
                    peso_total=0.0,
                )
            )
            continue

        valor_total = 0.0
        peso_total = 0.0

        for r in rows:
            # soma valor total (com fallback pelo SKU quando necessário)
            v = _valor_total_linha(r)
            qty = _qty_from_row(r)
            sku = str(r.get("SKU", "") or "").strip()
            if v <= 0.0 and sku:
                _, preco_fb = _resolver_peso_preco_sku(sku, qty, skus_info)
                v = preco_fb * max(qty, 1)
            valor_total += v

            # soma peso por SKU
            if sku:
                peso_unit, _ = _resolver_peso_preco_sku(sku, qty, skus_info)
                peso_total += peso_unit * max(qty, 1)

        if valor_total <= 0.0 or peso_total <= 0.0:
            resultados.append(
                ResultadoLote(
                    id_lote=id_lote,
                    email=email,
                    cep=cep8,
                    melhor=None,
                    todas=[],
                    mensagem="Total ou peso inválido para o lote",
                    valor_total=valor_total,
                    peso_total=peso_total,
                )
            )
            continue

        payload = _payload_fretebarato(cep8, valor_total, peso_total)
        try:
            r = sess.post(
                settings.FRETEBARATO_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=(5, 30),
            )
            r.raise_for_status()
            data = r.json() or {}
        except Exception as e:
            resultados.append(
                ResultadoLote(
                    id_lote=id_lote,
                    email=email,
                    cep=cep8,
                    melhor=None,
                    todas=[],
                    mensagem=f"Falha na cotação: {e}",
                    valor_total=valor_total,
                    peso_total=peso_total,
                )
            )
            continue

        quotes_raw = data.get("quotes", []) or []
        quotes = [q for q in quotes_raw if isinstance(q, Mapping)]
        compativeis = _filtrar_por_transportadoras(quotes, selecionadas_set)
        melhor = compativeis[0] if compativeis else None

        resultados.append(
            ResultadoLote(
                id_lote=id_lote,
                email=email,
                cep=cep8,
                melhor=melhor,
                todas=compativeis if req.incluir_todas_cotacoes else [],
                mensagem=None if melhor else "Nenhuma cotação compatível",
                valor_total=valor_total,
                peso_total=peso_total,
            )
        )

    return CotarFretesResponse(
        ok=True,
        resultados=resultados,
        total_lotes=len(resultados),
        total_com_frete=sum(1 for r in resultados if r.melhor is not None),
    )
