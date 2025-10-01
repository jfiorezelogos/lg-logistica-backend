from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from typing import Any, TypedDict

from app.schemas.shopify_vendas_produtos import ShopifyEnderecoResultado
from app.services.shopify_busca_bairro import buscar_cep_com_timeout
from app.utils.utils_helpers import (
    _normalizar_order_id,
    logger,
    normalizar_texto,
)

_BRASILIENSE_KEYS = ("SQS", "SQN", "SHIN", "SHIS", "SCLN", "SGAN", "SGAS", "SMLN", "SMAS")


def _is_brasilia_exception(city: str, uf: str, endereco_base: str) -> bool:
    uf = (uf or "").strip().upper()
    city_norm = (city or "").strip().lower()
    if uf != "DF":
        return False
    if "brasília" in city_norm or "brasilia" in city_norm:
        return True
    up = (endereco_base or "").upper()
    return any(k in up for k in _BRASILIENSE_KEYS)


# -----------------------------------------------------------------------------
# Enriquecimento: Parser/Normalização de endereço (+ LLM opcional)
# -----------------------------------------------------------------------------

_numero_pat = re.compile(r"(?:^|\s|,|-)N(?:º|o|\.)?\s*(\d+[A-Za-z]?)\b", flags=re.IGNORECASE)  # CHANGE: permite sufixo letra
_fim_numero_pat = re.compile(
    r"\b(\d{1,6}[A-Za-z]?)"  # CHANGE: permite 1 letra após o número
    r"(?:\s*(?:,|-|\s|apt\.?|apto\.?|bloco|casa|fundos|frente|sl|cj|q|qs)\b.*)?$",
    re.IGNORECASE,
)


def validar_endereco(address1: str) -> bool:
    # heurística simples: existe algum dígito na linha?
    return bool(re.search(r"\d", address1 or ""))


def registrar_log_norm_enderecos(order_id: str, resultado: Mapping[str, Any]) -> None:
    try:
        logger.info(
            "addr_norm_result",
            extra={"order_id": order_id, "resultado": json.dumps(dict(resultado), ensure_ascii=False)},
        )
    except Exception:
        logger.info("addr_norm_result", extra={"order_id": order_id})


def _remove_bairro_do_complemento(complemento: str, bairro_cep: str) -> str:
    if not complemento or not bairro_cep:
        return complemento or ""
    # remove ocorrência insensível e limpa separadores residuais
    comp = re.sub(re.escape(bairro_cep), "", complemento, flags=re.IGNORECASE)
    comp = re.sub(r"\s{2,}", " ", comp).strip(" ,-/")
    return comp


def _limpa_dup_base_no_complemento(complemento: str, base: str) -> str:
    if not complemento or not base:
        return complemento or ""
    comp = re.sub(re.escape(base), "", complemento, flags=re.IGNORECASE)
    comp = re.sub(r"\s{2,}", " ", comp).strip(" ,-/")
    return comp


def _parse_endereco(address1: str, address2: str = "") -> dict[str, str]:
    a1 = (address1 or "").strip()
    a2 = (address2 or "").strip()
    base = a1
    numero = ""
    compl = a2

    m = _numero_pat.search(a1)
    if m:
        numero = m.group(1)
        base = _numero_pat.sub("", a1).strip()
    else:
        m2 = _fim_numero_pat.search(a1)
        if m2:
            numero = m2.group(1)
            base = a1[: m2.start(1)].strip()
        else:
            return {"endereco_base": base, "numero": "", "complemento": a2 or "", "precisa_contato": "SIM"}

    resto = a1.replace(base, "", 1).replace(numero, "").strip(" ,-/")
    if resto and not compl:
        compl = resto

    return {
        "endereco_base": base.strip(" ,-/"),
        "numero": numero,
        "complemento": compl.strip(),
        "precisa_contato": "NÃO" if numero else "SIM",
    }


def parse_enderecos(enderecos: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for e in enderecos:
        k = str(e.get("id", "")).strip() or str(len(out) + 1)
        a1 = str(e.get("address1", "") or "")
        a2 = str(e.get("address2", "") or "")
        out[k] = _parse_endereco(a1, a2)
    return out


def _montar_prompt_gpt(address1: str, address2: str, logradouro_cep: str, bairro_cep: str, cidade_cep: str, uf_cep: str) -> str:  # CHANGE: inclui cidade/UF
    return f"""
Responda com um JSON contendo:

- base: nome oficial da rua (logradouro). Use `logradouro_cep` se existir. Caso contrário, extraia de `address1`.
- numero: número do imóvel. Deve ser um número puro (ex: "123") ou número com uma única letra (ex: "456B"). Use "s/n" se não houver número claro. O número deve aparecer logo após o nome da rua. **Nunca inclua bairros, nomes de edifícios, siglas ou outras palavras no número.**
- complemento: tudo que estiver em `address1` e `address2` que **não seja** o `base`, o `numero` ou o `bairro_cep`.
- precisa_contato: true apenas se `numero` for "s/n" **e** não for Brasília-DF.

Regras importantes:
- Nunca repita `base` no `complemento`.
- Nunca inclua palavras no `numero`.
- Nunca inclua `bairro_cep` no `complemento`.
- Use apenas as informações de `address1`, `address2` e `logradouro_cep`.

Dados fornecidos:
address1: {address1}
address2: {address2}
logradouro_cep: {logradouro_cep}
bairro_cep: {bairro_cep}
cidade_cep: {cidade_cep}
uf_cep: {uf_cep}

Formato de resposta:
{{"base": "...", "numero": "...", "complemento": "...", "precisa_contato": false}}
""".strip()


def normalizar_enderecos_gpt(
    *,
    address1: str,
    address2: str,
    logradouro_cep: str,
    bairro_cep: str,
    cidade_cep: str,   # CHANGE: passa cidade/UF para o prompt
    uf_cep: str,
    ai_provider: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    if ai_provider is None:
        return {}
    prompt = _montar_prompt_gpt(address1, address2, logradouro_cep, bairro_cep, cidade_cep, uf_cep)
    resp = ai_provider(prompt)
    if isinstance(resp, dict):
        return resp
    try:
        return json.loads(str(resp))
    except Exception:
        return {}


def normalizar_endereco_unico(
    *,
    order_id: str,
    address1: str,
    address2: str,
    cep: str | None = None,
    ai_provider: Callable[[str], Any] | None = None,
) -> ShopifyEnderecoResultado:
    pedido_id = _normalizar_order_id(order_id)

    # 1) CEP → logradouro/bairro/cidade/UF (para regra Brasília/DF e logradouro preferencial)
    cep_info = {}
    logradouro_cep = ""
    bairro_cep = ""
    cidade_cep = ""
    uf_cep = ""
    if cep:
        try:
            cep_info = buscar_cep_com_timeout(cep) or {}
            logradouro_cep = str(cep_info.get("street") or "")
            bairro_cep = str(cep_info.get("district") or "")
            cidade_cep = str(cep_info.get("city") or "")
            uf_cep = str(cep_info.get("uf") or "")
        except Exception as e:
            logger.error("addr_norm_cep_error", extra={"order_id": pedido_id, "err": str(e)})

    # 2) PRIMEIRO: parser determinístico por regex
    parsed = _parse_endereco(address1, address2)  # {"endereco_base","numero","complemento","precisa_contato"}
    base = parsed.get("endereco_base", "").strip(" ,-/")
    numero = (parsed.get("numero") or "").strip()
    complemento = (parsed.get("complemento") or "").strip()
    precisa = (parsed.get("precisa_contato") == "SIM")

    # 2.1) se não achou número, padronize para "s/n"
    if not numero:
        numero = "s/n"
        precisa = True

    # 2.2) privilegie logradouro oficial do CEP quando existir
    if logradouro_cep:
        if base and logradouro_cep and base.lower() != logradouro_cep.lower():
            base = logradouro_cep.strip()
            complemento = _limpa_dup_base_no_complemento(complemento, logradouro_cep)  # CHANGE: limpa base do complemento

    # 2.3) Nunca incluir bairro no complemento
    if bairro_cep and bairro_cep.lower() in complemento.lower():
        complemento = _remove_bairro_do_complemento(complemento, bairro_cep)  # CHANGE: sanitização consistente

    # 3) Exceção Brasília/DF: se numero == "s/n" e for Brasília, NÃO precisa contato
    if numero.lower() in {"s/n", "sn", "s-n"}:
        if _is_brasilia_exception(cidade_cep, uf_cep, base):
            precisa = False

    # 4) Fallback IA apenas se ainda estamos sem número real e provider ligado
    if numero.lower() in {"s/n", "sn", "s-n"} and ai_provider is not None:
        resp = normalizar_enderecos_gpt(
            address1=address1,
            address2=address2,
            logradouro_cep=logradouro_cep,
            bairro_cep=bairro_cep,
            cidade_cep=cidade_cep,  # CHANGE
            uf_cep=uf_cep,          # CHANGE
            ai_provider=ai_provider,
        )
        if resp:
            base_ai = str(resp.get("base", "") or "").strip()
            num_ai = str(resp.get("numero", "") or "").strip()
            comp_ai = str(resp.get("complemento", "") or "").strip()
            precisa_ai = bool(resp.get("precisa_contato", precisa))

            # valida número (apenas dígitos com opcional 1 letra)
            if re.match(r"^\d+[A-Za-z]?$", num_ai or ""):
                numero = num_ai
                precisa = precisa_ai
            # base: prioriza logradouro do CEP quando existir
            if base_ai:
                base = logradouro_cep.strip() if logradouro_cep else base_ai
            if comp_ai:
                complemento = _limpa_dup_base_no_complemento(comp_ai, base)  # CHANGE: remove duplicidade

            # se continuou s/n, reavalie exceção DF
            if numero.lower() in {"s/n", "sn", "s-n"}:
                if _is_brasilia_exception(cidade_cep, uf_cep, base):
                    precisa = False
                else:
                    precisa = True

    out: ShopifyEnderecoResultado = {
        "endereco_base": base or (address1 or "").strip(),
        "numero": numero or "s/n",
        "complemento": complemento,
        "precisa_contato": "NÃO" if not precisa else "SIM",
        "logradouro_oficial": logradouro_cep,
        "bairro_oficial": bairro_cep,
        "raw_address1": address1 or "",
        "raw_address2": address2 or "",
    }
    registrar_log_norm_enderecos(pedido_id, out)
    return out
