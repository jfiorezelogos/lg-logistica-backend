# shopify_planilha

from collections.abc import Callable
from typing import Any

from app.services.shopify_ajuste_endereco import normalizar_endereco_unico, parse_enderecos
from app.services.shopify_busca_bairro import _limpa_cep, obter_bairros_por_cep
from app.utils.utils_helpers import _normalizar_order_id


def enriquecer_cpfs_nas_linhas(linhas: list[dict[str, str]], mapa_cpfs: dict[str, str]) -> None:
    for l in linhas:
        if not l.get("CPF/CNPJ Comprador"):
            tid = _normalizar_order_id(l.get("transaction_id", ""))
            if tid and tid in mapa_cpfs:
                l["CPF/CNPJ Comprador"] = mapa_cpfs[tid]


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


def enriquecer_enderecos_nas_linhas(
    linhas: list[dict[str, Any]],
    *,
    ai_provider: Callable[[str], Any] | None = None,
) -> None:
    for l in linhas:
        address1 = str(l.get("Endereço Entrega") or l.get("Endereço Comprador") or "")
        address2 = str(l.get("Complemento Entrega") or l.get("Complemento Comprador") or "")
        numero_existente = str(l.get("Número Entrega") or l.get("Número Comprador") or "")
        if numero_existente and address1:
            continue
        cep = str(l.get("CEP Entrega") or l.get("CEP Comprador") or "")
        res = normalizar_endereco_unico(
            order_id=str(l.get("transaction_id", "")),
            address1=address1,
            address2=address2,
            cep=cep,
            ai_provider=ai_provider,
        )
        l["Endereço Comprador"] = res["endereco_base"]
        l["Número Comprador"] = res["numero"]
        l["Complemento Comprador"] = res["complemento"]
        l["Endereço Entrega"] = res["endereco_base"]
        l["Número Entrega"] = res["numero"]
        l["Complemento Entrega"] = res["complemento"]
        l["Precisa Contato"] = res["precisa_contato"]
        if res.get("bairro_oficial") and not str(l.get("Bairro Entrega", "")).strip():
            l["Bairro Entrega"] = res["bairro_oficial"]


def parse_enderecos_batch(enderecos: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return parse_enderecos(enderecos)
