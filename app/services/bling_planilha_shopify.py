# shopify_planilha

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from app.services.shopify_client import _coletar_remaining_lineitems
from app.services.viacep_client import _limpa_cep, obter_bairros_por_cep
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


def enriquecer_bairros_nas_linhas(linhas: list[dict[str, Any]]) -> None:
    ceps = {
        _limpa_cep(l.get("CEP Comprador", ""))
        for l in linhas
        if not str(l.get("Bairro Comprador", "")).strip() and _limpa_cep(l.get("CEP Comprador", ""))
    }
    if not ceps:
        return
    bairros, _ = obter_bairros_por_cep(ceps)
    if not bairros:
        return
    for l in linhas:
        cep_limp = _limpa_cep(l.get("CEP Comprador", ""))
        if cep_limp and not str(l.get("Bairro Comprador", "")).strip():
            if cep_limp in bairros:
                l["Bairro Comprador"] = bairros[cep_limp]
                if not str(l.get("Bairro Entrega", "")).strip():
                    l["Bairro Entrega"] = bairros[cep_limp]
