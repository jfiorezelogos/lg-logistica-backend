# shopify_planilha

from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any

from app.services.shopify_ajuste_endereco import (
    _limpa_cep,
    normalizar_endereco_unico,
    obter_bairros_por_cep,
    parse_enderecos,
)
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
    - Resolve CEPs distintos em lote com cache para eficiência.
    """
    if not linhas:
        return

    ceps: set[str] = set()

    if usar_cep_entrega:
        for l in linhas:
            if not str(l.get("Bairro Entrega", "")).strip():
                cl = _limpa_cep(l.get("CEP Entrega"))
                if len(cl) == 8:
                    ceps.add(cl)

    if usar_cep_comprador:
        for l in linhas:
            if not str(l.get("Bairro Comprador", "")).strip():
                cl = _limpa_cep(l.get("CEP Comprador"))
                if len(cl) == 8:
                    ceps.add(cl)

    # Resolve em lote (cacheado)
    bairros_map, _ = obter_bairros_por_cep(ceps, timeout=timeout)

    # Aplica sem sobrescrever quem já tem valor
    if usar_cep_entrega:
        for l in linhas:
            if not str(l.get("Bairro Entrega", "")).strip():
                cl = _limpa_cep(l.get("CEP Entrega"))
                bx = bairros_map.get(cl, "")
                if bx:
                    l["Bairro Entrega"] = bx

    if usar_cep_comprador:
        for l in linhas:
            if not str(l.get("Bairro Comprador", "")).strip():
                cl = _limpa_cep(l.get("CEP Comprador"))
                bx = bairros_map.get(cl, "")
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
