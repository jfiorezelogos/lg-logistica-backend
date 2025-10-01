from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any, cast

# primitives de coleta no Guru
from app.services.guru_client import (
    LIMITE_INFERIOR,
    TransientGuruError,
    coletar_vendas,
    coletar_vendas_com_retry,
    dividir_periodos_coleta_api_guru,
)

# helpers de data
from app.utils.datetime_helpers import _as_dt, _as_iso


def iniciar_coleta_vendas_produtos(
    *,
    data_ini: str | date,
    data_fim: str | date,
    nome_produto: str | None,
    skus_info: Mapping[str, Mapping[str, Any]],
    transportadoras_permitidas: Sequence[str] = (),
) -> dict[str, Any]:
    """
    Backend-only: prepara o payload de coleta de VENDAS de PRODUTOS.

    - `data_ini` / `data_fim`: ISO "YYYY-MM-DD" ou `date`
    - `nome_produto`: None → todos (exceto tipo 'assinatura')
    - `skus_info`: mapa de nome -> info (deve conter 'tipo' e opcionalmente 'guru_ids')
    - `transportadoras_permitidas`: nomes válidos no domínio (ex.: ["CORREIOS", "GFL", ...])

    Retorna o dict `payload` para ser consumido pela camada de domínio/worker/filas.
    Nenhuma dependência de UI ou estado global.
    """

    ini_iso = _as_iso(data_ini)
    fim_iso = _as_iso(data_fim)

    if ini_iso > fim_iso:
        raise ValueError("data_ini não pode ser posterior a data_fim.")

    # Seleção de produtos (exclui tipo 'assinatura')
    if nome_produto:
        info = skus_info.get(nome_produto, {})
        if str(info.get("tipo", "")).strip().lower() == "assinatura":
            raise ValueError(f"'{nome_produto}' é do tipo 'assinatura'; selecione apenas produtos.")
        produtos_alvo: dict[str, Mapping[str, Any]] = {nome_produto: info}
    else:
        produtos_alvo = {
            nome: info for nome, info in skus_info.items() if str(info.get("tipo", "")).strip().lower() != "assinatura"
        }

    # Extrai guru_ids como lista de strings
    produtos_ids: list[str] = []
    for info in produtos_alvo.values():
        gids: Sequence[Any] = cast(Sequence[Any], info.get("guru_ids", []))
        for gid in gids:
            s = str(gid).strip()
            if s:
                produtos_ids.append(s)

    if not produtos_ids:
        raise ValueError("Nenhum produto elegível com 'guru_ids' válidos encontrado para a coleta.")

    payload: dict[str, Any] = {
        "modo": "produtos",
        "inicio": ini_iso,
        "fim": fim_iso,
        "produtos_ids": produtos_ids,
        "transportadoras_permitidas": list(transportadoras_permitidas or []),
    }

    return payload


def preparar_coleta_vendas_produtos(
    data_ini: str,
    data_fim: str,
    nome_produto: str | None,
    *,
    skus_info: Mapping[str, Mapping[str, Any]],
    box_nome: str = "",
    transportadoras_permitidas: Sequence[str] = (),
) -> dict[str, Any]:
    """Prepara o payload para coleta de vendas de produtos (backend puro)."""

    ini_iso = _as_iso(data_ini)
    fim_iso = _as_iso(data_fim)

    # valida intervalo
    if ini_iso > fim_iso:
        raise ValueError("data_ini não pode ser posterior a data_fim.")

    # filtra produtos
    if nome_produto:
        info = skus_info.get(nome_produto, {})
        if str(info.get("tipo", "")).strip().lower() == "assinatura":
            raise ValueError(f"'{nome_produto}' é uma assinatura; selecione apenas produtos.")
        produtos_alvo: dict[str, Mapping[str, Any]] = {nome_produto: info}
    else:
        produtos_alvo = {
            nome: info for nome, info in skus_info.items() if str(info.get("tipo", "")).strip().lower() != "assinatura"
        }

    # extrai IDs do Guru
    produtos_ids: list[str] = []
    for info in produtos_alvo.values():
        gids = cast(Sequence[Any], info.get("guru_ids", []))
        for gid in gids:
            s = str(gid).strip()
            if s:
                produtos_ids.append(s)

    if not produtos_ids:
        raise ValueError("Nenhum produto elegível com 'guru_ids' válidos encontrado para a coleta.")

    return {
        "modo": "produtos",
        "inicio": ini_iso,
        "fim": fim_iso,
        "produtos_ids": produtos_ids,
        "box_nome": (box_nome or "").strip(),
        "transportadoras_permitidas": list(transportadoras_permitidas or []),
    }


def coletar_vendas_shopify(
    dados: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    """
    Coleta transações de PRODUTOS com base no payload:
      - dados['produtos_ids']: list[str]
      - dados['inicio'] / dados['fim']: "YYYY-MM-DD" (ou ISO com tempo)
    Usa janelas divididas por bimestres quadrimestrais (abr/ago/dez) via dividir_periodos_coleta_api_guru.
    Retorna (transacoes, reservado, dados_ecoado).
    """
    produtos_ids = list(map(str, dados.get("produtos_ids") or []))
    if not produtos_ids:
        raise ValueError("Payload inválido: 'produtos_ids' vazio.")

    # normaliza datas e aplica limite inferior de segurança
    ini_dt = _as_dt(dados.get("inicio"))
    end_dt = _as_dt(dados.get("fim"))
    if ini_dt > end_dt:
        raise ValueError("Intervalo inválido: inicio > fim.")

    # (opcional) clamp no limite inferior, se desejar
    ini_dt = max(ini_dt, LIMITE_INFERIOR)

    # divide o período em blocos
    blocos = dividir_periodos_coleta_api_guru(ini_dt, end_dt)  # -> [(ini_iso, fim_iso), ...]
    if not blocos:
        return [], {}, dict(dados)

    transacoes: list[dict[str, Any]] = []

    # execução simples (sequencial). Se quiser, paralelize como em assinaturas.
    for pid in produtos_ids:
        for ini_iso, fim_iso in blocos:
            try:
                pagina = coletar_vendas(pid, ini_iso, fim_iso)
                if pagina:
                    transacoes.extend(pagina)
            except TransientGuruError as e:
                # aplica retry externo com backoff
                pagina = coletar_vendas_com_retry(pid, ini_iso, fim_iso)
                if pagina:
                    transacoes.extend(pagina)
                else:
                    print(f"[⚠️] Produto {pid} sem dados após retries: {e}")

    return transacoes, {}, dict(dados)
