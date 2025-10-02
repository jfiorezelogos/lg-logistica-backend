from typing import Any

from pydantic import BaseModel, Field

# ============================
# Modelos básicos (mantidos)
# ============================


class Money(BaseModel):
    amount: float | None = None


class DiscountSet(BaseModel):
    shopMoney: Money | None = None


class ShippingLine(BaseModel):
    discountedPriceSet: DiscountSet | None = None


class Customer(BaseModel):
    email: str | None = None
    firstName: str | None = None
    lastName: str | None = None


class ShopifyEnderecoResultado(BaseModel):
    name: str | None = None
    address1: str | None = None
    address2: str | None = None
    city: str | None = None
    zip: str | None = None
    provinceCode: str | None = None
    phone: str | None = None
    bairro_oficial: str | None = None
    logradouro_oficial: str | None = None
    numero: str | None = None
    complemento: str | None = None
    precisa_contato: str | None = None  # "SIM" | "NÃO"


class LineItem(BaseModel):
    id: str | None = None
    title: str | None = None
    quantity: int | None = None
    sku: str | None = None
    product: dict[str, Any] | None = None
    discountedTotalSet: DiscountSet | None = None


class ShopifyPedido(BaseModel):
    id: str
    name: str | None = None
    createdAt: str | None = None
    displayFulfillmentStatus: str | None = None
    currentTotalDiscountsSet: DiscountSet | None = None
    customer: Customer | None = None
    shippingAddress: ShopifyEnderecoResultado | None = None
    shippingLine: ShippingLine | None = None
    lineItems: list[LineItem] = []
    cpf: str | None = None  # extraído de localizationExtensions, quando houver


# ===================================================
# Planilha (JSON) — estrutura padronizada p/ o Bling
# ===================================================


class LinhaPlanilhaBling(BaseModel):
    numero_pedido: str = Field(alias="Número pedido")
    nome_comprador: str = Field(alias="Nome Comprador")
    data_pedido: str = Field(alias="Data Pedido")
    data: str = Field(alias="Data")
    cpf_cnpj_comprador: str = Field(alias="CPF/CNPJ Comprador")
    endereco_comprador: str = Field(alias="Endereço Comprador")
    bairro_comprador: str | None = Field(default="", alias="Bairro Comprador")
    numero_comprador: str | None = Field(default="", alias="Número Comprador")
    complemento_comprador: str | None = Field(default="", alias="Complemento Comprador")
    cep_comprador: str = Field(alias="CEP Comprador")
    cidade_comprador: str = Field(alias="Cidade Comprador")
    uf_comprador: str = Field(alias="UF Comprador")
    telefone_comprador: str | None = Field(default="", alias="Telefone Comprador")
    celular_comprador: str | None = Field(default="", alias="Celular Comprador")
    email_comprador: str | None = Field(default="", alias="E-mail Comprador")

    produto: str = Field(alias="Produto")
    sku: str = Field(alias="SKU")
    un: str = Field(alias="Un")
    quantidade: str = Field(alias="Quantidade")
    valor_unitario: str = Field(alias="Valor Unitário")
    valor_total: str = Field(alias="Valor Total")
    total_pedido: str | None = Field(default="", alias="Total Pedido")
    valor_frete_pedido: str = Field(alias="Valor Frete Pedido")
    valor_desconto_pedido: str = Field(alias="Valor Desconto Pedido")
    outras_despesas: str | None = Field(default="", alias="Outras despesas")

    nome_entrega: str = Field(alias="Nome Entrega")
    endereco_entrega: str = Field(alias="Endereço Entrega")
    numero_entrega: str | None = Field(default="", alias="Número Entrega")
    complemento_entrega: str | None = Field(default="", alias="Complemento Entrega")
    cidade_entrega: str = Field(alias="Cidade Entrega")
    uf_entrega: str = Field(alias="UF Entrega")
    cep_entrega: str = Field(alias="CEP Entrega")
    bairro_entrega: str | None = Field(default="", alias="Bairro Entrega")

    transportadora: str | None = Field(default="", alias="Transportadora")
    servico: str | None = Field(default="", alias="Serviço")
    tipo_frete: str = Field(alias="Tipo Frete")
    observacoes: str | None = Field(default="", alias="Observações")
    qtd_parcela: str | None = Field(default="", alias="Qtd Parcela")
    data_prevista: str | None = Field(default="", alias="Data Prevista")
    vendedor: str | None = Field(default="", alias="Vendedor")
    forma_pagamento: str | None = Field(default="", alias="Forma Pagamento")
    id_forma_pagamento: str | None = Field(default="", alias="ID Forma Pagamento")

    transaction_id: str = Field(alias="transaction_id")
    id_line_item: str = Field(alias="id_line_item")
    id_produto: str = Field(alias="id_produto")
    indisponivel: str = Field(alias="indisponivel")
    precisa_contato: str = Field(alias="Precisa Contato")
    status_fulfillment: str = Field(alias="status_fulfillment")

    model_config = {
        "populate_by_name": True,
        "strict": False,
        "ignored_types": (),
    }


class PlanilhaFiltros(BaseModel):
    status: str
    data_inicio: str  # ISO (YYYY-MM-DD) recebido na rota


class PlanilhaBlingResponse(BaseModel):
    linhas: list[LinhaPlanilhaBling]
    stats: dict[str, Any]
    filtros: PlanilhaFiltros
