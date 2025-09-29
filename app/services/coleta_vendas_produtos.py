from __future__ import annotations

from datetime import date
from typing import Any, Mapping, Optional, Sequence, cast
from services.datetime_utils import _as_iso


def iniciar_coleta_vendas_produtos(
    *,
    data_ini: str | date,
    data_fim: str | date,
    nome_produto: Optional[str],
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
            nome: info
            for nome, info in skus_info.items()
            if str(info.get("tipo", "")).strip().lower() != "assinatura"
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
