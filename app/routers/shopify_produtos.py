from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.schemas.shopify_mapeamento import ProductShopifyVariant
from app.services.shopify_produtos import buscar_produtos_shopify

router = APIRouter(prefix="/shopify", tags=["Coleta"])


class ShopifyProdutosResponse(BaseModel):
    count: int = Field(..., description="Quantidade de variantes retornadas")
    data: list[ProductShopifyVariant]


@router.get(
    "/produtos",
    response_model=ShopifyProdutosResponse,
    summary="Listar produtos/variantes da Shopify",
    description=(
        "Retorna uma lista plana de variantes de produtos da Shopify, "
        "com `product_id`, `variant_id`, `title` e `sku`."
    ),
)
def listar_produtos_shopify(
    limit: int = Query(0, ge=0, description="Opcional: limitar a quantidade de itens retornados (0 = todos)"),
) -> ShopifyProdutosResponse:
    try:
        todos = buscar_produtos_shopify()
        if limit > 0:
            todos = todos[:limit]
        return ShopifyProdutosResponse(count=len(todos), data=todos)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Falha ao buscar produtos da Shopify: {e}")
