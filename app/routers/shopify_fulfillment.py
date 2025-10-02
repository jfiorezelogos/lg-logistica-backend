from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.schemas.shopify_fulfillment import FulfillBatchRequest, FulfillBatchResponse
from app.services.shopify_fulfillment import processar_fulfillments

router = APIRouter(prefix="/shopify", tags=["Controle de envios"])


@router.post("/fulfill", response_model=FulfillBatchResponse)
def fulfill_shopify(req: FulfillBatchRequest) -> FulfillBatchResponse:
    try:
        return processar_fulfillments(req)
    except Exception:
        # mensagem curta + 502
        raise HTTPException(status_code=502, detail="Erro ao criar fulfillments")
