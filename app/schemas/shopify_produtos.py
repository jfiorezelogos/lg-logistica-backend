from __future__ import annotations

from pydantic import BaseModel, Field


class ProductShopifyVariant(BaseModel):
    product_id: int = Field(..., description="ID do produto na Shopify")
    variant_id: int | None = Field(None, description="ID da variante (se houver)")
    title: str = Field(..., description="Título do produto na Shopify")
    sku: str = Field(..., description="SKU da variante/produto na Shopify")

    class Config:
        json_schema_extra = {
            "example": {
                "product_id": 123456789,
                "variant_id": 987654321,
                "title": "Camisa Azul - GG",
                "sku": "CAM-AZ-002",
            }
        }


class ShopifyProdutosResponse(BaseModel):
    count: int = Field(..., description="Quantidade de variantes retornadas")
    data: list[ProductShopifyVariant]
