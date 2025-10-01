# shopify_planilha

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from app.services.shopify_client import _coletar_remaining_lineitems
from app.utils.utils_helpers import _normalizar_order_id


def enriquecer_cpfs_nas_linhas(linhas: list[dict[str, str]], mapa_cpfs: dict[str, str]) -> None:
    for l in linhas:
        if not l.get("CPF/CNPJ Comprador"):
            tid = _normalizar_order_id(l.get("transaction_id", ""))
            if tid and tid in mapa_cpfs:
                l["CPF/CNPJ Comprador"] = mapa_cpfs[tid]


def _linhas_por_pedido(
    pedido: Mapping[str, Any],
    modo_fs: str,
    produto_alvo: str | None,
    skus_info: Mapping[str, Mapping[str, Any]],
    remaining_por_line: dict[str, int] | None = None,  # <-- novo
) -> list[dict[str, Any]]:
    # mapeia product_id -> (nome_produto, sku_interno) a partir do skus.json
    prod_map: dict[str, tuple[str, str]] = {}
    alvo = (produto_alvo or "").strip().lower()

    for nome_local, dados in skus_info.items():
        if alvo and alvo not in nome_local.lower():
            continue
        for sid in map(str, dados.get("shopify_ids", []) or []):
            prod_map[sid] = (nome_local, str(dados.get("sku", "")))

    cust = pedido.get("customer") or {}
    first = (cust.get("firstName") or "").strip()
    last = (cust.get("lastName") or "").strip()
    nome_cliente = f"{first} {last}".strip()
    email = cust.get("email") or ""
    endereco = pedido.get("shippingAddress") or {}
    telefone = endereco.get("phone") or ""
    transaction_id = str(pedido.get("id") or "").split("/")[-1]

    frete_any = ((pedido.get("shippingLine") or {}).get("discountedPriceSet") or {}).get("shopMoney", {})
    valor_frete = float(frete_any.get("amount") or 0)

    status_fulfillment = (pedido.get("displayFulfillmentStatus") or "").strip().upper()

    desc_any = (pedido.get("currentTotalDiscountsSet") or {}).get("shopMoney") or {}
    valor_desconto = float(desc_any.get("amount") or 0)

    remaining_por_line = _coletar_remaining_lineitems(pedido)

    linhas: list[dict[str, Any]] = []
    for item_edge in (pedido.get("lineItems") or {}).get("edges", []):
        item = (item_edge or {}).get("node") or {}
        product_gid = ((item.get("product") or {}) or {}).get("id", "")
        if not product_gid:
            continue
        product_id = str(product_gid).split("/")[-1]

        if alvo and product_id not in prod_map:
            continue

        nome_produto, sku_interno = prod_map.get(product_id, ("", ""))

        base_qtd = int(item.get("quantity") or 0)
        total_linha = float(((item.get("discountedTotalSet") or {}).get("shopMoney") or {}).get("amount") or 0)
        valor_unitario = round(total_linha / base_qtd, 2) if base_qtd else 0.0
        id_line_item = str(item.get("id") or "").split("/")[-1]

        if modo_fs == "unfulfilled":
            remaining = int((remaining_por_line or {}).get(id_line_item, 0))
            if remaining <= 0:
                continue
            qtd_a_gerar = remaining
        else:
            qtd_a_gerar = base_qtd if base_qtd > 0 else 0

        for _ in range(qtd_a_gerar):
            linhas.append(
                {
                    "Número pedido": pedido.get("name", ""),
                    "Nome Comprador": nome_cliente,
                    "Data Pedido": (pedido.get("createdAt") or "")[:10],
                    "Data": datetime.now().strftime("%d/%m/%Y"),
                    "CPF/CNPJ Comprador": "",
                    "Endereço Comprador": endereco.get("address1", ""),
                    "Bairro Comprador": endereco.get("district", ""),
                    "Número Comprador": endereco.get("number", ""),
                    "Complemento Comprador": endereco.get("address2", ""),
                    "CEP Comprador": endereco.get("zip", ""),
                    "Cidade Comprador": endereco.get("city", ""),
                    "UF Comprador": endereco.get("provinceCode", ""),
                    "Telefone Comprador": telefone,
                    "Celular Comprador": telefone,
                    "E-mail Comprador": email,
                    "Produto": nome_produto,
                    "SKU": sku_interno,
                    "Un": "UN",
                    "Quantidade": "1",
                    "Valor Unitário": f"{valor_unitario:.2f}".replace(".", ","),
                    "Valor Total": f"{valor_unitario:.2f}".replace(".", ","),
                    "Total Pedido": "",
                    "Valor Frete Pedido": f"{valor_frete:.2f}".replace(".", ","),
                    "Valor Desconto Pedido": f"{valor_desconto:.2f}".replace(".", ","),
                    "Outras despesas": "",
                    "Nome Entrega": nome_cliente,
                    "Endereço Entrega": endereco.get("address1", ""),
                    "Número Entrega": endereco.get("number", ""),
                    "Complemento Entrega": endereco.get("address2", ""),
                    "Cidade Entrega": endereco.get("city", ""),
                    "UF Entrega": endereco.get("provinceCode", ""),
                    "CEP Entrega": endereco.get("zip", ""),
                    "Bairro Entrega": endereco.get("district", ""),
                    "Transportadora": "",
                    "Serviço": "",
                    "Tipo Frete": "0 - Frete por conta do Remetente (CIF)",
                    "Observações": "",
                    "Qtd Parcela": "",
                    "Data Prevista": "",
                    "Vendedor": "",
                    "Forma Pagamento": "",
                    "ID Forma Pagamento": "",
                    "transaction_id": transaction_id,
                    "id_line_item": id_line_item,
                    "id_produto": product_id,
                    "indisponivel": "N",
                    "Precisa Contato": "SIM",
                    "status_fulfillment": status_fulfillment,
                }
            )
    return linhas


def _enriquecer_bairros_local(linhas: list[dict[str, Any]]) -> None:
    """
    Preenche 'Bairro Comprador' usando apenas campos do pedido (address2 e padrões em address1).
    Não faz chamadas externas.
    """
    import re as _re

    for l in linhas:
        if str(l.get("Bairro Comprador", "")).strip():
            continue
        cand = str(l.get("Complemento Entrega", "") or "").strip()
        if not cand:
            a1 = str(l.get("Endereço Entrega", "") or "")
            m = _re.search(r"\b(bairro|jd\.?|jardim|vl\.?|vila|centro)\s+([A-Za-zÀ-ÿ0-9\- ]+)", a1, flags=_re.I)
            if m:
                cand = (m.group(0) or "").strip()
        if cand:
            l["Bairro Comprador"] = cand


def _ajustar_enderecos_local(linhas: list[dict[str, Any]]) -> None:
    """
    Extrai número e complemento do 'Endereço Entrega' de forma determinística (regex),
    sem IA e sem consultar CEP/serviços.
    """
    import re as _re

    _numero_pat = _re.compile(r"(?:^|\s|,|-)N(?:º|o|\.)?\s*(\d+)\b", flags=_re.IGNORECASE)
    _fim_numero_pat = _re.compile(
        r"\b(\d{1,6})(?:\s*(?:,|-|\s|apt\.?|apto\.?|bloco|casa|fundos|frente|sl|cj|q|qs)\b.*)?$",
        _re.IGNORECASE,
    )

    for l in linhas:
        a1 = str(l.get("Endereço Entrega", "") or "").strip()
        a2 = str(l.get("Complemento Entrega", "") or "").strip()

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

        # se sobrar "resto" após remover número, aproveita como complemento se ainda vazio
        resto = a1.replace(base, "", 1)
        resto = resto.replace(numero, "").strip(" ,-/")
        if resto and not compl:
            compl = resto

        l["Endereço Entrega"] = base.strip(" ,-/")
        if numero:
            l["Número Entrega"] = numero
            l["Precisa Contato"] = "NÃO"
        else:
            l["Número Entrega"] = str(l.get("Número Entrega", "") or "").strip()
            if not l["Número Entrega"]:
                l["Precisa Contato"] = "SIM"
        if compl:
            l["Complemento Entrega"] = compl.strip()
