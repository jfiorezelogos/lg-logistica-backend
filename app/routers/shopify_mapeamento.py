from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.schemas.shopify_mapeamento import MapearShopifyRequest, MapearShopifyResponse
from app.services.shopify_mapeamento import mapear_produtos_shopify_service

router = APIRouter(prefix="/mapear", tags=["Mapeamento"])


@router.post(
    "/shopify-produtos",
    response_model=MapearShopifyResponse,
    summary="Mapear itens/variantes da Shopify a um SKU interno",
    description=(
        "Recebe o SKU interno (definido no skus.json) e uma lista de IDs de produto/variante da Shopify. "
        "Adiciona os IDs em 'shopify_ids' sem duplicar."
    ),
)
def mapear_produtos_shopify(req: MapearShopifyRequest) -> MapearShopifyResponse:
    try:
        out = mapear_produtos_shopify_service(sku=req.sku, shopify_ids_in=req.shopify_ids)
        return MapearShopifyResponse(**out)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao mapear produtos da Shopify: {e}")
