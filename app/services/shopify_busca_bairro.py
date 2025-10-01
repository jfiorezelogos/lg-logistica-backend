from __future__ import annotations

import re
from collections.abc import Iterable
from functools import lru_cache
from typing import Any

from brazilcep import exceptions as br_ex, get_address_from_cep

# --------------------------
# Utils de CEP / Normalização
# --------------------------

_CEP_RE = re.compile(r"\d{5}-?\d{3}")


def _limpa_cep(cep: str | None) -> str:
    """Mantém apenas dígitos e corta para 8 (formato ViaCEP)."""
    return re.sub(r"\D", "", str(cep or ""))[:8]


# --------------------------
# Busca unitária (com cache)
# --------------------------


@lru_cache(maxsize=4096)
def _buscar_endereco_cached(cep8: str, timeout: int = 5) -> dict[str, Any]:
    """
    Busca endereço no brazilcep para um CEP de 8 dígitos.
    Retorna sempre um dict (pode ser {} em caso de erro/CEP inexistente).
    """
    if not cep8 or len(cep8) != 8:
        return {}
    try:
        data = get_address_from_cep(cep8, timeout=timeout) or {}
        # Normaliza chaves esperadas
        return {
            "street": str(data.get("street") or "").strip(),
            "district": str(data.get("district") or "").strip(),
            "city": str(data.get("city") or data.get("localidade") or "").strip(),
            "uf": str(data.get("state") or data.get("uf") or "").strip(),
            "cep": cep8,
        }
    except br_ex.CEPNotFound:
        # CEP não encontrado — não loga excessivamente para não poluir
        return {}
    except Exception:
        # Qualquer outra falha de rede/parse
        return {}


def buscar_cep_com_timeout(cep: str, timeout: int = 5) -> dict[str, Any]:
    """Consulta um CEP com timeout usando brazilcep. Retorna {} em caso de erro."""
    cep8 = _limpa_cep(cep)
    return _buscar_endereco_cached(cep8, timeout=timeout) if len(cep8) == 8 else {}


# --------------------------
# Lote por coleção de CEPs
# --------------------------


def obter_bairros_por_cep(ceps: Iterable[str], timeout: int = 5) -> tuple[dict[str, str], dict[str, str]]:
    """
    Retorna (mapa_bairros, mapa_logradouros) por CEP usando brazilcep.
    - Dedupe automático de CEPs.
    - Usa cache interno para evitar chamadas repetidas.
    """
    bairros: dict[str, str] = {}
    logs: dict[str, str] = {}

    seen: set[str] = set()
    for c in ceps:
        cep8 = _limpa_cep(c)
        if len(cep8) != 8 or cep8 in seen:
            continue
        seen.add(cep8)

        data = _buscar_endereco_cached(cep8, timeout=timeout)
        if not data:
            continue

        if data.get("district"):
            bairros[cep8] = data["district"]
        if data.get("street"):
            logs[cep8] = data["street"]

    return bairros, logs


# --------------------------
# Enriquecedor direto nas linhas
# --------------------------


def enriquecer_bairros_nas_linhas(
    linhas: list[dict[str, Any]],
    *,
    usar_cep_entrega: bool = True,
    usar_cep_comprador: bool = True,
    timeout: int = 5,
) -> None:
    """
    Preenche 'Bairro Entrega' e 'Bairro Comprador' consultando brazilcep por CEP.
    - Não sobrescreve valores já preenchidos.
    - Opera in-place.
    - Usa cache LRU para evitar múltiplos hits do mesmo CEP.
    """
    # 1) Coleta todos os CEPs que precisam de bairro
    ceps_need: set[str] = set()

    if usar_cep_entrega:
        for l in linhas:
            if not str(l.get("Bairro Entrega", "")).strip():
                cep8 = _limpa_cep(l.get("CEP Entrega"))
                if len(cep8) == 8:
                    ceps_need.add(cep8)

    if usar_cep_comprador:
        for l in linhas:
            if not str(l.get("Bairro Comprador", "")).strip():
                cep8 = _limpa_cep(l.get("CEP Comprador"))
                if len(cep8) == 8:
                    ceps_need.add(cep8)

    # 2) Resolve em lote (cacheado)
    bairros_map, _ = obter_bairros_por_cep(ceps_need, timeout=timeout)

    # 3) Aplica nas linhas (sem sobrescrever quem já tem valor)
    if usar_cep_entrega:
        for l in linhas:
            if not str(l.get("Bairro Entrega", "")).strip():
                cep8 = _limpa_cep(l.get("CEP Entrega"))
                bx = bairros_map.get(cep8, "")
                if bx:
                    l["Bairro Entrega"] = bx

    if usar_cep_comprador:
        for l in linhas:
            if not str(l.get("Bairro Comprador", "")).strip():
                cep8 = _limpa_cep(l.get("CEP Comprador"))
                bx = bairros_map.get(cep8, "")
                if bx:
                    l["Bairro Comprador"] = bx
