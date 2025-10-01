# schemas/shopify_mapeamento.py
from __future__ import annotations

from pydantic import BaseModel, Field


class MapearShopifyRequest(BaseModel):
    sku: str = Field(..., description="SKU interno (chave no skus.json)")
    shopify_ids: list[int | str] = Field(..., description="Lista de IDs de produto/variante da Shopify")


class MapearShopifyResponse(BaseModel):
    sku: str
    shopify_ids: list[int | str]
    adicionados: int = Field(..., description="Quantidade de IDs adicionados nesse mapeamento")
    total_mapeados: int = Field(..., description="Total de IDs mapeados após a operação")
    message: str


__all__ = ["MapearShopifyRequest", "MapearShopifyResponse", "ProductShopifyVariant"]
