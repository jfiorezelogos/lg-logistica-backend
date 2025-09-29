# app/routers/guru_mapear_produtos.py
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.schemas.guru_mapeamento import MapearGuruRequest, MapearGuruResponse
from app.services.guru_mapeamento import mapear_produtos_guru

router = APIRouter(prefix="/mapear", tags=["Mapeamento"])


@router.post("/guru-produtos", response_model=MapearGuruResponse, summary="Mapear produtos do Guru para SKU interno")
def mapear_guru(req: MapearGuruRequest) -> MapearGuruResponse:
    try:
        out = mapear_produtos_guru(
            {
                "sku": req.sku,
                "tipo": req.tipo,
                "guru_ids": req.guru_ids,
                "recorrencia": req.recorrencia,
                "periodicidade": req.periodicidade,
            }
        )
        return MapearGuruResponse(**out)  # tipagem pydantic
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao mapear produtos: {e}")
