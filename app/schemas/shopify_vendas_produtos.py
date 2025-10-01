from __future__ import annotations

from typing import Any, TypedDict

from pydantic import BaseModel


class ShopifyPedido(TypedDict, total=False):
    id: str
    name: str
    createdAt: str
    customer: dict[str, Any]
    shippingAddress: dict[str, Any]
    displayFulfillmentStatus: str
    shippingLine: dict[str, Any]
    currentTotalDiscountsSet: dict[str, Any]
    lineItems: dict[str, Any]
    fulfillmentOrders: dict[str, Any]


class ShopifyEnderecoResultado(TypedDict, total=False):
    endereco_base: str
    numero: str
    complemento: str
    precisa_contato: str  # "SIM" | "NÃO"
    logradouro_oficial: str
    bairro_oficial: str
    raw_address1: str
    raw_address2: str


class ShopifyColetaPedidosIn(BaseModel):
    data_inicio: str
    fulfillment_status: str = "any"
    produto_alvo: str | None = None
    ids_shopify: list[str] | None = None

    enrich_cpfs: bool = True
    enrich_bairros: bool = True
    enrich_enderecos: bool = True

    # NOVO: liga/desliga uso de LLM para endereço
    use_ai_enderecos: bool = True


class ShopifyColetaPedidosOut(BaseModel):
    linhas: list[dict[str, Any]]
    contagem: dict[str, dict[str, int]]
