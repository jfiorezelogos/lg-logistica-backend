from __future__ import annotations

from pydantic import BaseModel, Field


class FulfillPedidoIn(BaseModel):
    transaction_id: str = Field(..., description="Order numeric ID (ex.: '7036106703038')")
    line_item_ids: list[str] = Field(
        default_factory=list,
        description="Lista de IDs numéricos de LineItem (ex.: ['16020280803518', ...])",
    )


class FulfillBatchRequest(BaseModel):
    pedidos: list[FulfillPedidoIn]
    notify_customer: bool = Field(False, description="Se true, Shopify notifica o cliente")


class FulfillOrderResult(BaseModel):
    order_id: str
    fulfilled_count: int = 0
    ignored_line_items: list[str] = Field(
        default_factory=list, description="LineItems solicitados mas ignorados (não pendentes)"
    )
    message: str | None = Field(default=None, description="Mensagem de erro ou observação")


class FulfillBatchResponse(BaseModel):
    ok: bool
    total_fulfilled: int
    results: list[FulfillOrderResult]
