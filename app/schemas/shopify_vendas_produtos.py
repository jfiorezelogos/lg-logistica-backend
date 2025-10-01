from typing import Any

from pydantic import BaseModel, Field


# ---------- Query params ----------
class ShopifyPedidosQuery(BaseModel):
    status: str = Field(default="any", pattern="^(any|unfulfilled)$")
    data_inicio: str = Field(..., description="YYYY-MM-DD (filtra created_at >= data_inicio)")
    cursor: str | None = Field(default=None, description="Cursor de paginação da Shopify")
    limit: int = Field(default=50, ge=1, le=50, description="Máximo por página")
    normalize_endereco: bool = Field(default=False, description="Enriquecer endereço usando serviços de ajuste")


# ---------- Modelos básicos ----------
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


# ---------- Pedido (nó principal) ----------
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


# ---------- Resposta paginada ----------
class PedidosResponse(BaseModel):
    items: list[ShopifyPedido]
    next_cursor: str | None = None
