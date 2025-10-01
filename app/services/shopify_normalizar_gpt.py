import json
import re
from collections.abc import Callable, Mapping
from typing import Any, TypedDict

from app.schemas.shopify_vendas_produtos import EnderecoResultado
from app.services.viacep_client import buscar_cep_com_timeout
from app.utils.utils_helpers import (
    _normalizar_order_id,
    logger,
    normalizar_texto,
    registrar_log_norm_enderecos,
    validar_endereco,
)


# -----------------------------------------------------------------------------
# Enriquecimento: Parser/Normalização de endereço (+ LLM opcional)
# -----------------------------------------------------------------------------
class EnderecoResultado(TypedDict, total=False):
    endereco_base: str
    numero: str
    complemento: str
    precisa_contato: str  # "SIM" | "NÃO"
    logradouro_oficial: str
    bairro_oficial: str
    raw_address1: str
    raw_address2: str


_numero_pat = re.compile(r"(?:^|\s|,|-)N(?:º|o|\.)?\s*(\d+)\b", flags=re.IGNORECASE)
_fim_numero_pat = re.compile(
    r"\b(\d{1,6})(?:\s*(?:,|-|\s|apt\.?|apto\.?|bloco|casa|fundos|frente|sl|cj|q|qs)\b.*)?$",
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


def normalizar_enderecos_batch(
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


def _montar_prompt_gpt(address1: str, address2: str, logradouro_cep: str, bairro_cep: str) -> str:
    return f"""
Responda com um JSON contendo:

- base: nome oficial da rua (logradouro). Use `logradouro_cep` se existir. Caso contrário, extraia de `address1`.
- numero: número do imóvel. Deve ser um número puro (ex: "123") ou número com uma única letra (ex: "456B"). Use "s/n" se não houver número claro. O número deve aparecer logo após o nome da rua. **Nunca inclua bairros, nomes de edifícios, siglas ou outras palavras no número.**
- complemento: tudo que estiver em `address1` e `address2` que **não seja** o `base`, o `numero` ou o `bairro_cep`.
- precisa_contato: true apenas se `numero` for "s/n" e o cep nao for de Brasília-DF

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

Formato de resposta:
{{"base": "...", "numero": "...", "complemento": "...", "precisa_contato": false}}
""".strip()


def normalizar_enderecos_gpt(
    *,
    address1: str,
    address2: str,
    logradouro_cep: str,
    bairro_cep: str,
    ai_provider: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    if ai_provider is None:
        return {}
    prompt = _montar_prompt_gpt(address1, address2, logradouro_cep, bairro_cep)
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
) -> EnderecoResultado:
    pedido_id = _normalizar_order_id(order_id)
    logradouro_cep = ""
    bairro_cep = ""

    if cep:
        try:
            cep_info = buscar_cep_com_timeout(cep)
            logradouro_cep = str(cep_info.get("street") or "")
            bairro_cep = str(cep_info.get("district") or "")
        except Exception as e:
            logger.error("addr_norm_cep_error", extra={"order_id": pedido_id, "err": str(e)})

    precisa = False
    base = ""
    numero = ""
    complemento = (address2 or "").strip()

    if validar_endereco(address1):
        partes = [p.strip() for p in (address1 or "").split(",", 1)]
        base = partes[0]
        numero = (partes[1] if len(partes) > 1 else "").strip() or "s/n"
        precisa = numero == "s/n"
    else:
        resposta = normalizar_enderecos_gpt(
            address1=address1,
            address2=address2,
            logradouro_cep=logradouro_cep,
            bairro_cep=bairro_cep,
            ai_provider=ai_provider,
        )
        if not resposta:
            parsed = _parse_endereco(address1, address2)
            base = parsed["endereco_base"]
            numero = parsed["numero"]
            complemento = parsed["complemento"]
            precisa = parsed["precisa_contato"] == "SIM"
        else:
            base = str(resposta.get("base", "") or "").strip()
            numero = str(resposta.get("numero", "") or "").strip()
            if not re.match(r"^\d+[A-Za-z]?$", numero):
                numero = "s/n"
                precisa = True
            comp_resp = str(resposta.get("complemento", "") or address2 or "").strip()
            complemento = "" if comp_resp.strip() == numero.strip() else comp_resp
            precisa = bool(resposta.get("precisa_contato", precisa))
            if logradouro_cep:
                base_norm = normalizar_texto(base)
                log_norm = normalizar_texto(logradouro_cep)
                if log_norm not in base_norm:
                    base = logradouro_cep.strip()

    out: EnderecoResultado = {
        "endereco_base": base,
        "numero": numero or "s/n",
        "complemento": complemento,
        "precisa_contato": "SIM" if precisa else "NÃO",
        "logradouro_oficial": logradouro_cep,
        "bairro_oficial": bairro_cep,
        "raw_address1": address1 or "",
        "raw_address2": address2 or "",
    }
    registrar_log_norm_enderecos(pedido_id, out)
    return out
